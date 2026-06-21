use super::*;
use chrono::Utc;

use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
    ObserverRef, SchemaRef, SemVer, SupplementalId, SupplementalRecord,
    supplemental::InputAnchorSet,
};

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
        source_system: None,
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
        }),
    }
}

#[test]
fn persist_and_reload_observation() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
    let observation = sample_observation();
    let mut collision = observation.clone();
    collision.id = Observation::new_id();
    collision.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "sample-key",
            "body": "changed"
        }).to_string(),
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
fn rehome_mode_a_preserves_stored_identity_and_times() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
    let mut first = sample_observation();
    first.idempotency_key = IdempotencyKey::new("first");
    first.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "first",
            "body": "first"
        }).to_string(),
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();

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
fn split_prepare_is_logged_without_changing_replayed_tree() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();

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
    let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
    let orphan = blob_dir.join("f".repeat(64));
    fs::write(&orphan, b"orphan").unwrap();

    let removed = store.garbage_collect_orphan_blobs().unwrap();
    assert_eq!(removed, 1);
    assert!(!orphan.exists());

    let _ = fs::remove_dir_all(tmp);
}
