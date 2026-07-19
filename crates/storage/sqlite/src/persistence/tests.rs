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
             BEFORE INSERT ON projection_materialization_items
             WHEN NEW.projection_id = 'proj:item-publish-target'
                  AND NEW.item_key = 'stage-b'
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
