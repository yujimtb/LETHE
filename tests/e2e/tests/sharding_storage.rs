use std::collections::BTreeSet;

use chrono::Utc;
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Observation, ObserverRef,
    ProjectionRef, SchemaRef, SemVer, SourceSystemRef,
};
use lethe_engine::propagation::idempotent::CommutativeIdempotentObservationFold;
use lethe_engine::propagation::scheduler::PropagationScheduler;
use lethe_storage_sqlite::persistence::SqlitePersistence;

#[derive(Default)]
struct Seen(BTreeSet<String>);

impl CommutativeIdempotentObservationFold for Seen {
    fn apply(&mut self, observation: &Observation) -> Result<(), String> {
        self.0.insert(observation.id.as_str().to_owned());
        Ok(())
    }
}

fn observation(key: &str) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:test"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:test"),
        source_system: Some(SourceSystemRef::new("sys:test")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("entity:{key}")),
        target: None,
        payload: serde_json::json!({"key": key}),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(key),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({"key": key}).to_string(),
            "source_container": "test",
        }),
    }
}

#[test]
fn persistent_propagation_reads_leaf_tail_and_commits_watermark() {
    let tmp = std::env::temp_dir().join(format!("lethe-e2e-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    store.persist_observation(&observation("one")).unwrap();
    store.persist_observation(&observation("two")).unwrap();

    let projection = ProjectionRef::new("proj:propagation-test");
    let mut fold = Seen::default();
    let first =
        PropagationScheduler::propagate_persistent(&projection, &store, 1, &mut fold).unwrap();
    assert_eq!(first.iter().map(|tail| tail.new_records).sum::<usize>(), 2);
    assert_eq!(fold.0.len(), 2);

    let second =
        PropagationScheduler::propagate_persistent(&projection, &store, 1, &mut fold).unwrap();
    assert!(second.is_empty());
    assert_eq!(fold.0.len(), 2);

    let _ = std::fs::remove_dir_all(tmp);
}
