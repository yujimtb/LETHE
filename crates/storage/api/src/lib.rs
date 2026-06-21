use lethe_core::domain::{
    BlobRef, IdempotencyKey, Observation, ObservationId, ProjectionRef, SupplementalRecord,
};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{0}")]
    Backend(String),
    #[error("storage invariant violation: {0}")]
    Invariant(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended(ObservationId),
    Duplicate(ObservationId),
    CanonicalCollision(ObservationId),
}

#[derive(Debug, Clone)]
pub struct StoredObservation {
    pub leaf_id: String,
    pub append_seq: u64,
    pub observation: Observation,
}

#[derive(Debug, Clone)]
pub enum RehomeMode {
    StoredIdentity,
    RecomputedIdentity {
        identity_key: IdempotencyKey,
        canonical_json: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafPosition {
    pub leaf_id: String,
    pub append_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionLeafWatermark {
    pub projection_id: ProjectionRef,
    pub leaf_id: String,
    pub append_seq: u64,
    pub status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncMetricRecord {
    pub fetched: u64,
    pub ingested: u64,
    pub skipped: u64,
    pub failed: u64,
    pub quarantined: u64,
    pub latency_ms: u64,
}

pub trait ObservationStore: Send {
    fn append_observation(&self, observation: &Observation) -> StorageResult<AppendOutcome>;
    fn rehome_observation(
        &self,
        observation: &Observation,
        mode: RehomeMode,
    ) -> StorageResult<AppendOutcome>;
    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>>;
    fn observations_for_leaf_after(
        &self,
        leaf_id: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>>;
    fn observation_by_id(&self, id: &ObservationId) -> StorageResult<Option<StoredObservation>>;
    fn leaf_positions(&self) -> StorageResult<Vec<LeafPosition>>;
    fn split_leaf_if_capacity(&self, capacity: usize) -> StorageResult<bool>;
}

pub trait BlobStore: Send {
    fn put_blob(&self, data: &[u8], max_bytes: usize) -> StorageResult<BlobRef>;
    fn get_blob(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>>;
}

pub trait SupplementalStore: Send {
    fn put_supplemental(&self, record: &SupplementalRecord) -> StorageResult<()>;
    fn supplemental_page(
        &self,
        after_created_at: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<SupplementalRecord>>;
}

pub trait ProjectionMaterializer: Send {
    fn materialize_projection(
        &self,
        projection: &ProjectionRef,
        records: &serde_json::Value,
    ) -> StorageResult<()>;
    fn projection_records(
        &self,
        projection: &ProjectionRef,
    ) -> StorageResult<Option<serde_json::Value>>;
}

pub trait RuntimeStateStore: Send {
    fn get_state(&self, key: &str) -> StorageResult<Option<String>>;
    fn set_state(&self, key: &str, value: &str) -> StorageResult<()>;
    fn record_dead_letter(&self, source: &str, reason: &str) -> StorageResult<()>;
    fn record_audit_event(
        &self,
        id: &str,
        timestamp: &str,
        actor: &str,
        event_json: &str,
    ) -> StorageResult<()>;
    fn record_sync_metrics(&self, source: &str, metrics: &SyncMetricRecord) -> StorageResult<()>;
    fn apply_retention(&self, retention_days: u32) -> StorageResult<usize>;
    fn garbage_collect_orphan_blobs(&self) -> StorageResult<usize>;
    fn deep_check(&self) -> StorageResult<()>;
}

pub trait ProjectionWatermarkStore: Send {
    fn projection_leaf_watermark(
        &self,
        projection: &ProjectionRef,
        leaf_id: &str,
    ) -> StorageResult<ProjectionLeafWatermark>;
    fn commit_projection_leaf_watermark(
        &self,
        watermark: &ProjectionLeafWatermark,
    ) -> StorageResult<()>;
}

pub trait StoragePorts:
    ObservationStore
    + BlobStore
    + SupplementalStore
    + ProjectionMaterializer
    + RuntimeStateStore
    + ProjectionWatermarkStore
{
}

impl<T> StoragePorts for T where
    T: ObservationStore
        + BlobStore
        + SupplementalStore
        + ProjectionMaterializer
        + RuntimeStateStore
        + ProjectionWatermarkStore
{
}

pub mod conformance {
    use super::*;
    use chrono::Utc;
    use lethe_core::domain::{
        AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
    };

    pub fn sample_observation(key: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:conformance"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:conformance"),
            source_system: Some(SourceSystemRef::new("sys:conformance")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("entity:conformance"),
            target: None,
            payload: serde_json::json!({"value": key}),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({"value": key}).to_string(),
                "source_container": "conformance",
            }),
        }
    }

    pub fn observation_store_round_trip<T: ObservationStore>(store: &T) {
        let observation = sample_observation("conformance:observation");
        assert!(matches!(
            store.append_observation(&observation).unwrap(),
            AppendOutcome::Appended(_)
        ));
        let loaded = store.observation_by_id(&observation.id).unwrap().unwrap();
        assert_eq!(loaded.observation.id, observation.id);
        assert!(matches!(
            store.append_observation(&observation).unwrap(),
            AppendOutcome::Duplicate(_)
        ));
    }

    pub fn blob_store_round_trip<T: BlobStore>(store: &T) {
        let blob = store.put_blob(b"conformance", 1024).unwrap();
        assert_eq!(
            store.get_blob(&blob).unwrap(),
            Some(b"conformance".to_vec())
        );
    }

    pub fn materializer_round_trip<T: ProjectionMaterializer>(materializer: &T) {
        let projection = ProjectionRef::new("proj:conformance");
        let records = serde_json::json!({"records": [1, 2, 3]});
        materializer
            .materialize_projection(&projection, &records)
            .unwrap();
        assert_eq!(
            materializer.projection_records(&projection).unwrap(),
            Some(records)
        );
    }
}
