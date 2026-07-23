use chrono::{DateTime, Utc};
use lethe_core::domain::{
    AuthorityModel, BlobRef, CaptureModel, DataSpaceId, IdempotencyKey, Observation, ObservationId,
    OperationalEventId, ProjectionRef, SupplementalId, SupplementalRecord,
};
use ring::hmac;
use serde::{Deserialize, Serialize};
use sha2::Digest;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{0}")]
    Backend(String),
    #[error("storage invariant violation: {0}")]
    Invariant(String),
    #[error("operational idempotency collision for {0}")]
    OperationalIdempotencyCollision(String),
    #[error("operational event_id collision for {0}")]
    OperationalEventIdCollision(String),
    #[error("cutover admission denied: {0}")]
    CutoverAdmissionDenied(String),
    #[error("cutover conflict: {0}")]
    CutoverConflict(String),
    #[error("cutover rollback refused: {0}")]
    CutoverRollbackRefused(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationalEvent {
    pub event_id: OperationalEventId,
    pub data_space_id: DataSpaceId,
    pub stream_id: String,
    pub stream_version: u64,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<OperationalEventId>,
    pub observation: Observation,
}

impl OperationalEvent {
    pub fn validate(&self) -> StorageResult<()> {
        validate_non_blank("event_id", self.event_id.as_str())?;
        validate_non_blank("data_space_id", self.data_space_id.as_str())?;
        validate_non_blank("stream_id", &self.stream_id)?;
        validate_non_blank("event_type", &self.event_type)?;
        validate_non_blank("actor_type", &self.actor_type)?;
        if self.stream_version == 0 {
            return Err(StorageError::Invariant(
                "operational event stream_version must be >= 1".to_owned(),
            ));
        }
        if self.observation.authority_model != AuthorityModel::LakeAuthoritative {
            return Err(StorageError::Invariant(
                "operational event observation must be lake_authoritative".to_owned(),
            ));
        }
        if self.observation.capture_model != CaptureModel::Event {
            return Err(StorageError::Invariant(
                "operational event observation capture_model must be event".to_owned(),
            ));
        }
        if self.observation.published != self.occurred_at {
            return Err(StorageError::Invariant(
                "operational event occurred_at must equal observation.published".to_owned(),
            ));
        }
        let meta = self.observation.meta.as_object().ok_or_else(|| {
            StorageError::Invariant(
                "operational event observation.meta must be an object".to_owned(),
            )
        })?;
        if meta
            .get("data_space_id")
            .and_then(serde_json::Value::as_str)
            != Some(self.data_space_id.as_str())
        {
            return Err(StorageError::Invariant(
                "operational event observation.meta.data_space_id mismatch".to_owned(),
            ));
        }
        if meta.get("event_id").and_then(serde_json::Value::as_str) != Some(self.event_id.as_str())
        {
            return Err(StorageError::Invariant(
                "operational event observation.meta.event_id mismatch".to_owned(),
            ));
        }
        validate_non_blank(
            "operational event observation.idempotency_key",
            self.observation.idempotency_key.as_str(),
        )
    }
}

fn validate_non_blank(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        return Err(StorageError::Invariant(format!(
            "operational event {field} must not be blank"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationalAppendRequest {
    pub event: OperationalEvent,
    pub expected_stream_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum OperationalAppendOutcome {
    Appended { cursor: u64, stream_version: u64 },
    Duplicate { cursor: u64, stream_version: u64 },
    VersionConflict { expected: u64, actual: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredOperationalEvent {
    pub cursor: u64,
    pub event: OperationalEvent,
}

/// Indexed predicates supported by the operational-event read path.
///
/// The cursor remains the canonical append sequence.  Each predicate is backed
/// by a composite `(data_space_id, predicate, cursor)` index in the storage
/// backends, so filtering never requires a cursor-zero client scan.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationalEventFilter {
    pub correlation_id: Option<String>,
    pub causation_id: Option<OperationalEventId>,
    pub event_type: Option<String>,
    pub stream_id: Option<String>,
    pub actor_id: Option<String>,
    pub occurred_at_from: Option<DateTime<Utc>>,
    pub occurred_at_to: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalEventStats {
    pub count: u64,
    pub max_cursor: u64,
}

pub trait OperationalEventStore: Send {
    fn data_space_id(&self) -> &DataSpaceId;

    fn append_operational_events(
        &self,
        requests: &[OperationalAppendRequest],
    ) -> StorageResult<Vec<OperationalAppendOutcome>>;

    fn append_operational_event(
        &self,
        request: &OperationalAppendRequest,
    ) -> StorageResult<OperationalAppendOutcome> {
        let mut outcomes = self.append_operational_events(std::slice::from_ref(request))?;
        outcomes.pop().ok_or_else(|| {
            StorageError::Invariant("operational event store returned no append outcome".to_owned())
        })
    }

    fn operational_event_stats(&self) -> StorageResult<OperationalEventStats>;

    fn operational_event_page(
        &self,
        after_cursor: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredOperationalEvent>>;

    fn operational_events_by_filter(
        &self,
        filter: &OperationalEventFilter,
        after_cursor: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredOperationalEvent>>;

    fn operational_events_for_stream(
        &self,
        stream_id: &str,
        after_stream_version: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredOperationalEvent>>;

    fn operational_event_by_id(
        &self,
        event_id: &OperationalEventId,
    ) -> StorageResult<Option<StoredOperationalEvent>>;

    fn operational_stream_version(&self, stream_id: &str) -> StorageResult<u64>;
}

pub trait OperationalStoragePorts: OperationalEventStore + BlobStore {}

impl<T> OperationalStoragePorts for T where T: OperationalEventStore + BlobStore {}

pub const OPERATIONAL_ARCHIVE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveBlobDigest {
    pub blob_ref: BlobRef,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalOperationalArchive {
    pub format_version: u32,
    pub data_space_id: DataSpaceId,
    pub exported_at: DateTime<Utc>,
    pub events: Vec<OperationalEvent>,
    pub blobs: Vec<ArchiveBlobDigest>,
}

impl CanonicalOperationalArchive {
    pub fn validate(&self) -> StorageResult<()> {
        if self.format_version != OPERATIONAL_ARCHIVE_FORMAT_VERSION {
            return Err(StorageError::Invariant(format!(
                "unsupported operational archive format_version: {}",
                self.format_version
            )));
        }
        validate_non_blank("archive data_space_id", self.data_space_id.as_str())?;
        let mut stream_versions = std::collections::BTreeMap::<String, u64>::new();
        let mut event_ids = std::collections::BTreeSet::new();
        for event in &self.events {
            event.validate()?;
            if event.data_space_id != self.data_space_id {
                return Err(StorageError::Invariant(
                    "archive contains an event from another data space".to_owned(),
                ));
            }
            if !event_ids.insert(event.event_id.as_str().to_owned()) {
                return Err(StorageError::Invariant(format!(
                    "archive contains duplicate event_id {}",
                    event.event_id
                )));
            }
            let expected = stream_versions.get(&event.stream_id).copied().unwrap_or(0) + 1;
            if event.stream_version != expected {
                return Err(StorageError::Invariant(format!(
                    "archive stream {} expected version {}, got {}",
                    event.stream_id, expected, event.stream_version
                )));
            }
            stream_versions.insert(event.stream_id.clone(), event.stream_version);
        }
        let mut blob_refs = std::collections::BTreeSet::new();
        for blob in &self.blobs {
            validate_non_blank("archive blob_ref", blob.blob_ref.as_str())?;
            if blob.sha256.len() != 64 || !blob.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(StorageError::Invariant(format!(
                    "archive blob {} has invalid sha256",
                    blob.blob_ref
                )));
            }
            let expected_ref = format!("blob:sha256:{}", blob.sha256.to_ascii_lowercase());
            if blob.blob_ref.as_str() != expected_ref {
                return Err(StorageError::Invariant(format!(
                    "archive blob reference {} does not match digest {}",
                    blob.blob_ref, blob.sha256
                )));
            }
            if !blob_refs.insert(blob.blob_ref.as_str().to_owned()) {
                return Err(StorageError::Invariant(format!(
                    "archive contains duplicate blob {}",
                    blob.blob_ref
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedOperationalArchive {
    pub manifest_json: String,
    pub hmac_sha256: String,
}

pub fn sign_operational_archive(
    archive: &CanonicalOperationalArchive,
    signing_key: &[u8],
) -> StorageResult<SignedOperationalArchive> {
    if signing_key.is_empty() {
        return Err(StorageError::Invariant(
            "operational archive signing key must not be empty".to_owned(),
        ));
    }
    archive.validate()?;
    let manifest_json =
        serde_json::to_string(archive).map_err(|error| StorageError::Backend(error.to_string()))?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    let signature = hmac::sign(&key, manifest_json.as_bytes());
    Ok(SignedOperationalArchive {
        manifest_json,
        hmac_sha256: hex::encode(signature.as_ref()),
    })
}

pub fn verify_operational_archive(
    signed: &SignedOperationalArchive,
    signing_key: &[u8],
) -> StorageResult<CanonicalOperationalArchive> {
    if signing_key.is_empty() {
        return Err(StorageError::Invariant(
            "operational archive signing key must not be empty".to_owned(),
        ));
    }
    let signature = hex::decode(&signed.hmac_sha256)
        .map_err(|error| StorageError::Invariant(format!("invalid archive signature: {error}")))?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    hmac::verify(&key, signed.manifest_json.as_bytes(), &signature).map_err(|_| {
        StorageError::Invariant("operational archive signature mismatch".to_owned())
    })?;
    let archive: CanonicalOperationalArchive = serde_json::from_str(&signed.manifest_json)
        .map_err(|error| StorageError::Invariant(format!("invalid archive manifest: {error}")))?;
    archive.validate()?;
    Ok(archive)
}

pub fn export_operational_archive<T: OperationalEventStore>(
    store: &T,
    exported_at: DateTime<Utc>,
    blobs: Vec<ArchiveBlobDigest>,
) -> StorageResult<CanonicalOperationalArchive> {
    let mut cursor = 0;
    let mut events = Vec::new();
    loop {
        let page = store.operational_event_page(cursor, 512)?;
        if page.is_empty() {
            break;
        }
        cursor = page
            .last()
            .map(|stored| stored.cursor)
            .ok_or_else(|| StorageError::Invariant("empty event page".to_owned()))?;
        events.extend(page.into_iter().map(|stored| stored.event));
    }
    let archive = CanonicalOperationalArchive {
        format_version: OPERATIONAL_ARCHIVE_FORMAT_VERSION,
        data_space_id: store.data_space_id().clone(),
        exported_at,
        events,
        blobs,
    };
    archive.validate()?;
    Ok(archive)
}

pub fn operational_blob_manifest<T: BlobStore>(
    store: &T,
    blob_refs: &[BlobRef],
) -> StorageResult<Vec<ArchiveBlobDigest>> {
    let mut unique = blob_refs.to_vec();
    unique.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    unique.dedup_by(|left, right| left.as_str() == right.as_str());
    unique
        .into_iter()
        .map(|blob_ref| {
            let data = store.get_blob(&blob_ref)?.ok_or_else(|| {
                StorageError::Invariant(format!(
                    "operational archive references missing blob {blob_ref}"
                ))
            })?;
            let sha256 = hex::encode(sha2::Sha256::digest(&data));
            let expected_ref = format!("blob:sha256:{sha256}");
            if blob_ref.as_str() != expected_ref {
                return Err(StorageError::Invariant(format!(
                    "blob content digest does not match reference {blob_ref}"
                )));
            }
            let bytes = u64::try_from(data.len()).map_err(|_| {
                StorageError::Invariant(format!("blob {blob_ref} length does not fit u64"))
            })?;
            Ok(ArchiveBlobDigest {
                blob_ref,
                sha256,
                bytes,
            })
        })
        .collect()
}

pub fn verify_operational_archive_blobs<T: BlobStore>(
    store: &T,
    archive: &CanonicalOperationalArchive,
) -> StorageResult<()> {
    archive.validate()?;
    for expected in &archive.blobs {
        let data = store.get_blob(&expected.blob_ref)?.ok_or_else(|| {
            StorageError::Invariant(format!(
                "operational archive blob {} is missing",
                expected.blob_ref
            ))
        })?;
        let bytes = u64::try_from(data.len()).map_err(|_| {
            StorageError::Invariant(format!(
                "blob {} length does not fit u64",
                expected.blob_ref
            ))
        })?;
        let actual_sha256 = hex::encode(sha2::Sha256::digest(&data));
        if bytes != expected.bytes || actual_sha256 != expected.sha256 {
            return Err(StorageError::Invariant(format!(
                "operational archive blob {} failed digest verification",
                expected.blob_ref
            )));
        }
    }
    Ok(())
}

pub fn replay_operational_archive<T: OperationalEventStore>(
    store: &T,
    archive: &CanonicalOperationalArchive,
) -> StorageResult<Vec<OperationalAppendOutcome>> {
    archive.validate()?;
    if store.data_space_id() != &archive.data_space_id {
        return Err(StorageError::Invariant(format!(
            "archive data space {} does not match target {}",
            archive.data_space_id,
            store.data_space_id()
        )));
    }
    let requests = archive
        .events
        .iter()
        .cloned()
        .map(|event| OperationalAppendRequest {
            expected_stream_version: event.stream_version - 1,
            event,
        })
        .collect::<Vec<_>>();
    store.append_operational_events(&requests)
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationStats {
    pub count: u64,
    pub max_append_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEventRecord {
    pub id: String,
    pub timestamp: String,
    pub actor: String,
    pub event_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEventCursor {
    pub timestamp: String,
    pub id: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedSyncState {
    pub metrics: SyncMetricRecord,
    pub completed_at: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlackThreadKey {
    pub source_instance: String,
    pub channel_id: String,
    pub thread_ts: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredSlackThread {
    pub key: SlackThreadKey,
    pub observation_append_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackThreadCatalogEntry {
    pub key: SlackThreadKey,
    pub reply_cursor: String,
    pub active: bool,
    pub next_poll_generation: u64,
    pub discovered_append_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionItem {
    pub item_key: String,
    pub owner_key: String,
    pub sort_key: String,
    pub value: serde_json::Value,
}

impl ProjectionItem {
    pub fn validate(&self) -> StorageResult<()> {
        validate_projection_item_key("item_key", &self.item_key)?;
        validate_projection_item_key("owner_key", &self.owner_key)?;
        validate_projection_item_key("sort_key", &self.sort_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionItemCommit {
    Replace {
        items: Vec<ProjectionItem>,
    },
    Delta {
        inserts: Vec<ProjectionItem>,
        updates: Vec<ProjectionItem>,
        deletes: Vec<String>,
    },
}

impl ProjectionItemCommit {
    pub fn validate(&self) -> StorageResult<()> {
        let mut operations = std::collections::BTreeMap::new();
        match self {
            Self::Replace { items } => {
                validate_projection_item_operations(items, "replace", &mut operations)?;
            }
            Self::Delta {
                inserts,
                updates,
                deletes,
            } => {
                validate_projection_item_operations(inserts, "insert", &mut operations)?;
                validate_projection_item_operations(updates, "update", &mut operations)?;
                for item_key in deletes {
                    validate_projection_item_key("delete item_key", item_key)?;
                    register_projection_item_operation(item_key, "delete", &mut operations)?;
                }
            }
        }
        Ok(())
    }
}

fn validate_projection_item_operations(
    items: &[ProjectionItem],
    operation: &'static str,
    operations: &mut std::collections::BTreeMap<String, &'static str>,
) -> StorageResult<()> {
    for item in items {
        item.validate()?;
        register_projection_item_operation(&item.item_key, operation, operations)?;
    }
    Ok(())
}

fn register_projection_item_operation(
    item_key: &str,
    operation: &'static str,
    operations: &mut std::collections::BTreeMap<String, &'static str>,
) -> StorageResult<()> {
    if let Some(previous) = operations.insert(item_key.to_owned(), operation) {
        return Err(StorageError::Invariant(if previous == operation {
            format!("projection item commit contains duplicate {operation} item_key {item_key}")
        } else {
            format!(
                "projection item commit contains conflicting operations {previous} and {operation} for item_key {item_key}"
            )
        }));
    }
    Ok(())
}

fn validate_projection_item_key(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        return Err(StorageError::Invariant(format!(
            "projection item {field} must not be blank"
        )));
    }
    Ok(())
}

pub trait ObservationStore: Send {
    fn append_observation(&self, observation: &Observation) -> StorageResult<AppendOutcome>;
    fn append_observations(
        &self,
        observations: &[Observation],
    ) -> StorageResult<Vec<AppendOutcome>> {
        observations
            .iter()
            .map(|observation| self.append_observation(observation))
            .collect()
    }
    fn append_observations_with_audit(
        &self,
        observations: &[Observation],
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<Vec<AppendOutcome>>;
    fn load_observations(&self) -> StorageResult<Vec<Observation>>;
    fn observation_stats(&self) -> StorageResult<ObservationStats>;
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

/// Durable storage operations for the v1/v2 cutover bridge.
///
/// These operations are deliberately separate from the frozen v1 observation
/// port.  The bridge is an admission and v2-resolution concern; v1 callers
/// must opt into the fenced append method explicitly.
pub trait CutoverStore: Send {
    fn append_observations_v1_with_admission(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observations: &[Observation],
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<Vec<AppendOutcome>>;

    fn append_slack_observation_v1_with_admission(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observation: &Observation,
        thread: &SlackThreadKey,
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<AppendOutcome>;

    fn append_observations_v2_with_bridge(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observations: &[Observation],
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<Vec<AppendOutcome>>;

    fn cutover_admit(
        &self,
        source_instance_id: &str,
        api_version: CutoverApiVersion,
        generation: Option<u64>,
    ) -> StorageResult<()>;

    fn identity_bridge_apply_batch(
        &self,
        batch_size: usize,
    ) -> StorageResult<IdentityBridgeBatchReport>;
    fn identity_bridge_watermark(&self) -> StorageResult<u64>;
    fn identity_bridge_resolve(
        &self,
        v2_identity_key: &str,
        canonical_json: &str,
    ) -> StorageResult<IdentityBridgeResolution>;

    fn cutover_register(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState>;
    fn cutover_state(&self, source_instance_id: &str) -> StorageResult<CutoverState>;
    fn cutover_inventory(&self) -> StorageResult<Vec<CutoverInventoryItem>>;
    fn cutover_begin_drain(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState>;
    fn cutover_readiness(
        &self,
        source_instance_id: &str,
        fixture: Option<&CutoverFixture>,
    ) -> StorageResult<CutoverReadinessReport>;
    fn cutover_activate(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
        fixture: &CutoverFixture,
    ) -> StorageResult<CutoverState>;
    fn cutover_rollback(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState>;
    fn cutover_health(&self, source_instance_id: &str) -> StorageResult<CutoverHealth>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutoverApiVersion {
    V1,
    V2,
}

impl CutoverApiVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutoverPhase {
    V1Active,
    Draining,
    V2Active,
    V2Committed,
}

impl CutoverPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1Active => "v1_active",
            Self::Draining => "draining",
            Self::V2Active => "v2_active",
            Self::V2Committed => "v2_committed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverState {
    pub source_instance_id: String,
    pub phase: CutoverPhase,
    pub generation: u64,
    pub fence_append_seq: Option<u64>,
    pub first_v2_append_seq: Option<u64>,
    pub v2_ingested: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityBridgeBatchReport {
    pub previous_watermark: u64,
    pub watermark: u64,
    pub read_count: usize,
    pub candidate_count: usize,
    pub gap_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityBridgeResolution {
    pub v2_identity_key: String,
    pub winner: Option<ObservationId>,
    pub winner_append_seq: Option<u64>,
    pub multiplicity: u64,
    pub canonical_collision: bool,
    pub collision_append_seq: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverFixture {
    pub object_id: String,
    pub canonical_json: String,
    pub expected_identity_key: String,
    pub expected_observation_id: Option<ObservationId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverBlocker {
    pub append_seq: Option<u64>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverReadinessReport {
    pub state: CutoverState,
    pub bridge_watermark: u64,
    pub bridge_lag: u64,
    pub watermark_covered: bool,
    pub unresolved_gap_count: u64,
    pub exact_compare_error_count: u64,
    pub fixture_identity_stable: bool,
    pub dry_run_passed: bool,
    pub candidate_count: u64,
    pub multiplicity_count: u64,
    pub collision_count: u64,
    pub blockers: Vec<CutoverBlocker>,
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverInventoryItem {
    pub source_instance_id: String,
    pub observation_count: u64,
    pub producer_ids: Vec<String>,
    pub credential_ids: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverHealth {
    pub state: CutoverState,
    pub bridge_watermark: u64,
    pub bridge_lag: u64,
    pub candidate_count: u64,
    pub gap_count: u64,
    pub multiplicity_count: u64,
    pub collision_count: u64,
    pub bridge_duplicate_hit_count: u64,
    pub stale_v1_rejection_count: u64,
}

pub trait BlobStore: Send {
    fn put_blob(&self, data: &[u8], max_bytes: usize) -> StorageResult<BlobRef>;
    fn put_blobs(&self, data: &[&[u8]], max_bytes: usize) -> StorageResult<Vec<BlobRef>>;
    fn get_blob(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>>;
}

pub trait SupplementalStore: Send {
    fn put_supplemental(&self, record: &SupplementalRecord) -> StorageResult<()>;
    fn load_supplementals(&self) -> StorageResult<Vec<SupplementalRecord>>;
    fn supplemental_by_id(&self, id: &SupplementalId) -> StorageResult<Option<SupplementalRecord>>;
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
    fn commit_projection_items(
        &self,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        commit: &ProjectionItemCommit,
    ) -> StorageResult<()>;
    fn publish_projection_items_from_staging(
        &self,
        target: &ProjectionRef,
        staging: &ProjectionRef,
        manifest: &serde_json::Value,
        expected_item_count: u64,
    ) -> StorageResult<()>;
    fn projection_item_by_key(
        &self,
        projection: &ProjectionRef,
        item_key: &str,
    ) -> StorageResult<Option<ProjectionItem>>;
    fn projection_items_by_owner(
        &self,
        projection: &ProjectionRef,
        owner_key: &str,
    ) -> StorageResult<Vec<ProjectionItem>>;
    fn projection_items_page(
        &self,
        projection: &ProjectionRef,
        owner_keys: &[String],
        item_key_prefix: Option<&str>,
        after_sort_key: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<ProjectionItem>>;
    fn projection_blob_ref_visible(
        &self,
        projection: &ProjectionRef,
        blob_ref: &BlobRef,
    ) -> StorageResult<bool>;
    fn projection_item_count_by_owner(
        &self,
        projection: &ProjectionRef,
        owner_key: &str,
    ) -> StorageResult<u64>;
    fn projection_item_count(&self, projection: &ProjectionRef) -> StorageResult<u64>;
}

pub trait SupplementalProjectionCommitter: Send {
    fn commit_supplemental_and_projection(
        &self,
        record: &SupplementalRecord,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
    ) -> StorageResult<()>;
    fn commit_supplemental_and_projection_with_audit(
        &self,
        record: &SupplementalRecord,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
        audit_event: &AuditEventRecord,
    ) -> StorageResult<()>;
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
    fn audit_event_page(
        &self,
        after: Option<&AuditEventCursor>,
        limit: usize,
    ) -> StorageResult<Vec<AuditEventRecord>>;
    fn record_sync_metrics(&self, source: &str, metrics: &SyncMetricRecord) -> StorageResult<()>;
    fn record_sync_state(&self, source: &str, state: &PersistedSyncState) -> StorageResult<()>;
    fn load_sync_state(&self, source: &str) -> StorageResult<Option<PersistedSyncState>>;
    fn apply_retention(&self, retention_days: u32) -> StorageResult<usize>;
    fn garbage_collect_orphan_blobs(&self) -> StorageResult<usize>;
    fn deep_check(&self) -> StorageResult<()>;
}

pub trait SlackThreadCatalogStore: Send {
    fn append_slack_observation(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
    ) -> StorageResult<AppendOutcome>;
    fn append_slack_observation_with_audit(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<AppendOutcome>;
    fn slack_thread_discovery_high_water(&self) -> StorageResult<u64>;
    fn commit_slack_thread_discovery(
        &self,
        high_water: u64,
        threads: &[DiscoveredSlackThread],
    ) -> StorageResult<()>;
    fn advance_slack_thread_poll_generation(&self) -> StorageResult<u64>;
    fn slack_threads_to_poll(
        &self,
        source_instance: &str,
        channel_id: &str,
        generation: u64,
        limit: usize,
    ) -> StorageResult<Vec<SlackThreadCatalogEntry>>;
    fn complete_slack_thread_poll(
        &self,
        key: &SlackThreadKey,
        generation: u64,
        reply_cursor: &str,
        active: bool,
        next_poll_generation: u64,
    ) -> StorageResult<()>;
    fn slack_thread_catalog(
        &self,
        source_instance: &str,
        channel_id: &str,
    ) -> StorageResult<Vec<SlackThreadCatalogEntry>>;
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
    + CutoverStore
    + BlobStore
    + SupplementalStore
    + ProjectionMaterializer
    + SupplementalProjectionCommitter
    + RuntimeStateStore
    + SlackThreadCatalogStore
    + ProjectionWatermarkStore
{
}

impl<T> StoragePorts for T where
    T: ObservationStore
        + CutoverStore
        + BlobStore
        + SupplementalStore
        + ProjectionMaterializer
        + SupplementalProjectionCommitter
        + RuntimeStateStore
        + SlackThreadCatalogStore
        + ProjectionWatermarkStore
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn item(item_key: &str) -> ProjectionItem {
        ProjectionItem {
            item_key: item_key.to_owned(),
            owner_key: "owner".to_owned(),
            sort_key: "sort".to_owned(),
            value: serde_json::json!({"item_key": item_key}),
        }
    }

    #[test]
    fn projection_item_delta_accepts_disjoint_explicit_operations() {
        ProjectionItemCommit::Delta {
            inserts: vec![item("insert")],
            updates: vec![item("update")],
            deletes: vec!["delete".to_owned()],
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn projection_item_delta_rejects_duplicate_and_conflicting_operations() {
        let invalid = [
            ProjectionItemCommit::Delta {
                inserts: vec![item("same"), item("same")],
                updates: vec![],
                deletes: vec![],
            },
            ProjectionItemCommit::Delta {
                inserts: vec![],
                updates: vec![item("same"), item("same")],
                deletes: vec![],
            },
            ProjectionItemCommit::Delta {
                inserts: vec![],
                updates: vec![],
                deletes: vec!["same".to_owned(), "same".to_owned()],
            },
            ProjectionItemCommit::Delta {
                inserts: vec![item("same")],
                updates: vec![item("same")],
                deletes: vec![],
            },
            ProjectionItemCommit::Delta {
                inserts: vec![item("same")],
                updates: vec![],
                deletes: vec!["same".to_owned()],
            },
            ProjectionItemCommit::Delta {
                inserts: vec![],
                updates: vec![item("same")],
                deletes: vec!["same".to_owned()],
            },
        ];

        for commit in invalid {
            assert!(matches!(commit.validate(), Err(StorageError::Invariant(_))));
        }
    }

    #[test]
    fn operational_archive_signature_detects_tampering() {
        let archive = CanonicalOperationalArchive {
            format_version: OPERATIONAL_ARCHIVE_FORMAT_VERSION,
            data_space_id: DataSpaceId::new("space:personal"),
            exported_at: Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap(),
            events: vec![],
            blobs: vec![],
        };
        let signed = sign_operational_archive(&archive, b"archive-test-key").unwrap();
        let verified = verify_operational_archive(&signed, b"archive-test-key").unwrap();
        assert_eq!(verified.data_space_id, archive.data_space_id);

        let mut tampered = signed;
        tampered.manifest_json = tampered
            .manifest_json
            .replace("space:personal", "space:company");
        assert!(verify_operational_archive(&tampered, b"archive-test-key").is_err());
    }

    #[test]
    fn operational_archive_rejects_blob_reference_digest_mismatch() {
        let archive = CanonicalOperationalArchive {
            format_version: OPERATIONAL_ARCHIVE_FORMAT_VERSION,
            data_space_id: DataSpaceId::new("space:personal"),
            exported_at: Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap(),
            events: vec![],
            blobs: vec![ArchiveBlobDigest {
                blob_ref: BlobRef::new(format!("blob:sha256:{}", "a".repeat(64))),
                sha256: "b".repeat(64),
                bytes: 1,
            }],
        };
        assert!(archive.validate().is_err());
    }
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

    pub fn sample_operational_event(
        data_space_id: &DataSpaceId,
        event_id: &str,
        stream_id: &str,
        stream_version: u64,
        idempotency_key: &str,
    ) -> OperationalEvent {
        let occurred_at = Utc::now();
        OperationalEvent {
            event_id: OperationalEventId::new(event_id),
            data_space_id: data_space_id.clone(),
            stream_id: stream_id.to_owned(),
            stream_version,
            event_type: "work_item_created".to_owned(),
            occurred_at,
            actor_type: "human".to_owned(),
            actor_id: Some("owner".to_owned()),
            correlation_id: Some("correlation:conformance".to_owned()),
            causation_id: None,
            observation: Observation {
                id: Observation::new_id(),
                schema: SchemaRef::new("schema:nanihold-operational-event"),
                schema_version: SemVer::new("1.0.0"),
                observer: ObserverRef::new("obs:nanihold-kernel"),
                source_system: Some(SourceSystemRef::new("sys:nanihold")),
                actor: Some(EntityRef::new("human:owner")),
                authority_model: AuthorityModel::LakeAuthoritative,
                capture_model: CaptureModel::Event,
                subject: EntityRef::new("work:conformance"),
                target: None,
                payload: serde_json::json!({"title": "conformance"}),
                attachments: vec![],
                published: occurred_at,
                recorded_at: occurred_at,
                consent: None,
                idempotency_key: IdempotencyKey::new(idempotency_key),
                meta: serde_json::json!({
                    "canonical_json": serde_json::json!({
                        "event_id": event_id,
                        "stream_id": stream_id,
                        "stream_version": stream_version,
                        "title": "conformance"
                    }).to_string(),
                    "source_container": "nanihold",
                    "data_space_id": data_space_id.as_str(),
                    "event_id": event_id
                }),
            },
        }
    }

    pub fn operational_event_store_round_trip<T: OperationalEventStore>(store: &T) {
        assert_eq!(
            store.operational_event_stats().unwrap(),
            OperationalEventStats {
                count: 0,
                max_cursor: 0
            }
        );
        let first = sample_operational_event(
            store.data_space_id(),
            "event:conformance:1",
            "work:conformance",
            1,
            "operational:conformance:1",
        );
        let request = OperationalAppendRequest {
            event: first.clone(),
            expected_stream_version: 0,
        };
        assert!(matches!(
            store.append_operational_event(&request).unwrap(),
            OperationalAppendOutcome::Appended {
                stream_version: 1,
                ..
            }
        ));
        assert!(matches!(
            store.append_operational_event(&request).unwrap(),
            OperationalAppendOutcome::Duplicate {
                stream_version: 1,
                ..
            }
        ));

        let conflict = OperationalAppendRequest {
            event: sample_operational_event(
                store.data_space_id(),
                "event:conformance:conflict",
                "work:conformance",
                1,
                "operational:conformance:conflict",
            ),
            expected_stream_version: 0,
        };
        assert_eq!(
            store.append_operational_event(&conflict).unwrap(),
            OperationalAppendOutcome::VersionConflict {
                expected: 0,
                actual: 1
            }
        );

        let second = OperationalAppendRequest {
            event: sample_operational_event(
                store.data_space_id(),
                "event:conformance:2",
                "work:conformance",
                2,
                "operational:conformance:2",
            ),
            expected_stream_version: 1,
        };
        assert!(matches!(
            store.append_operational_event(&second).unwrap(),
            OperationalAppendOutcome::Appended {
                stream_version: 2,
                ..
            }
        ));
        assert_eq!(
            store
                .operational_stream_version("work:conformance")
                .unwrap(),
            2
        );
        let page = store.operational_event_page(0, 10).unwrap();
        assert_eq!(page.len(), 2);
        assert!(page[0].cursor < page[1].cursor);
        let stream = store
            .operational_events_for_stream("work:conformance", 1, 10)
            .unwrap();
        assert_eq!(stream.len(), 1);
        assert_eq!(stream[0].event.stream_version, 2);
        assert_eq!(
            store
                .operational_event_by_id(&OperationalEventId::new("event:conformance:2"))
                .unwrap()
                .unwrap()
                .event
                .event_id
                .as_str(),
            "event:conformance:2"
        );
        assert_eq!(
            store.operational_event_stats().unwrap(),
            OperationalEventStats {
                count: 2,
                max_cursor: page[1].cursor
            }
        );
    }

    pub fn observation_store_round_trip<T: ObservationStore>(store: &T) {
        assert_eq!(
            store.observation_stats().unwrap(),
            ObservationStats {
                count: 0,
                max_append_seq: 0,
            }
        );

        let observation = sample_observation("conformance:observation");
        assert!(matches!(
            store.append_observation(&observation).unwrap(),
            AppendOutcome::Appended(_)
        ));
        let stats_after_append = store.observation_stats().unwrap();
        assert_eq!(stats_after_append.count, 1);
        assert!(stats_after_append.max_append_seq > 0);
        let loaded = store.observation_by_id(&observation.id).unwrap().unwrap();
        assert_eq!(loaded.observation.id, observation.id);
        assert!(matches!(
            store.append_observation(&observation).unwrap(),
            AppendOutcome::Duplicate(_)
        ));

        let bulk = vec![sample_observation("conformance:bulk")];
        assert!(matches!(
            store.append_observations(&bulk).unwrap().as_slice(),
            [AppendOutcome::Appended(_)]
        ));
        let stats_after_bulk = store.observation_stats().unwrap();
        assert_eq!(stats_after_bulk.count, 2);
        assert!(stats_after_bulk.max_append_seq > stats_after_append.max_append_seq);
    }

    pub fn blob_store_round_trip<T: BlobStore>(store: &T) {
        let blob = store.put_blob(b"conformance", 1024).unwrap();
        assert_eq!(
            store.get_blob(&blob).unwrap(),
            Some(b"conformance".to_vec())
        );

        let batch = store
            .put_blobs(&[b"batch-b", b"batch-a", b"batch-b"], 1024)
            .unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0], batch[2]);
        assert_ne!(batch[0], batch[1]);
        assert_eq!(
            batch[0].as_str(),
            format!(
                "blob:sha256:{}",
                hex::encode(sha2::Sha256::digest(b"batch-b"))
            )
        );
        assert_eq!(
            store.get_blob(&batch[0]).unwrap(),
            Some(b"batch-b".to_vec())
        );
        assert_eq!(
            store.get_blob(&batch[1]).unwrap(),
            Some(b"batch-a".to_vec())
        );

        let repeated = store
            .put_blobs(&[b"batch-b", b"batch-a", b"batch-b"], 1024)
            .unwrap();
        assert_eq!(repeated, batch);

        assert!(matches!(
            store.put_blobs(&[b"ok", b"too-large"], 3),
            Err(StorageError::Invariant(_))
        ));
        assert_eq!(store.put_blobs(&[], 1024).unwrap(), Vec::<BlobRef>::new());
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

        let manifest = serde_json::json!({"version": 1, "watermark": 2});
        let items = vec![
            ProjectionItem {
                item_key: "item-b".to_owned(),
                owner_key: "owner-a".to_owned(),
                sort_key: "002".to_owned(),
                value: serde_json::json!({"body": "b"}),
            },
            ProjectionItem {
                item_key: "item-a".to_owned(),
                owner_key: "owner-a".to_owned(),
                sort_key: "001".to_owned(),
                value: serde_json::json!({"body": "a"}),
            },
        ];
        materializer
            .commit_projection_items(
                &projection,
                &manifest,
                &ProjectionItemCommit::Replace {
                    items: items.clone(),
                },
            )
            .unwrap();
        assert_eq!(
            materializer.projection_records(&projection).unwrap(),
            Some(manifest)
        );
        assert_eq!(
            materializer
                .projection_items_by_owner(&projection, "owner-a")
                .unwrap()
                .into_iter()
                .map(|item| item.item_key)
                .collect::<Vec<_>>(),
            vec!["item-a", "item-b"]
        );
        assert_eq!(
            materializer
                .projection_item_count_by_owner(&projection, "owner-a")
                .unwrap(),
            2
        );
        assert_eq!(materializer.projection_item_count(&projection).unwrap(), 2);
        assert_eq!(
            materializer
                .projection_item_by_key(&projection, "item-a")
                .unwrap(),
            Some(items[1].clone())
        );

        let staging = ProjectionRef::new("proj:conformance-staging");
        materializer
            .commit_projection_items(
                &staging,
                &serde_json::json!({"page": 0}),
                &ProjectionItemCommit::Replace { items: vec![] },
            )
            .unwrap();
        materializer
            .commit_projection_items(
                &staging,
                &serde_json::json!({"page": 1}),
                &ProjectionItemCommit::Delta {
                    inserts: vec![items[0].clone()],
                    updates: vec![],
                    deletes: vec![],
                },
            )
            .unwrap();
        let published_manifest = serde_json::json!({"version": 2, "item_count": 1});
        materializer
            .publish_projection_items_from_staging(&projection, &staging, &published_manifest, 1)
            .unwrap();
        assert_eq!(
            materializer.projection_records(&projection).unwrap(),
            Some(published_manifest)
        );
        assert_eq!(materializer.projection_item_count(&projection).unwrap(), 1);
        assert_eq!(materializer.projection_records(&staging).unwrap(), None);
        assert_eq!(materializer.projection_item_count(&staging).unwrap(), 0);
    }
}
