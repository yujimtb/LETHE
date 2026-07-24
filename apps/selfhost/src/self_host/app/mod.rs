use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Arc, Mutex,
    mpsc::{self, Receiver, SyncSender, TrySendError},
};

use arc_swap::ArcSwap;
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::attribute_inventory::{AttributeInventoryDocument, build_inventory_documents};
use crate::self_host::config::{
    GoogleConfig, OperationalLedgerConfig, SelfHostConfig, SlackConfig,
};
use crate::self_host::google::HttpGoogleSlidesClient;
use crate::self_host::registry::{seed_projection_catalog, seed_registry};
use crate::self_host::slack::HttpSlackClient;
use lethe_adapter_api::config::{
    AdapterConfig, BackoffStrategy, RateLimitConfig, RetryConfig, SchemaBinding,
};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::idempotency::{CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, identity_key};
use lethe_adapter_api::retry::ResilientExecutor;
use lethe_adapter_api::traits::{ObservationDraft, SourceAdapter};
use lethe_adapter_gslides::gslides::client::GoogleSlidesClient;
use lethe_adapter_gslides::gslides::mapper::GoogleSlidesAdapter;
use lethe_adapter_slack::slack::client::SlackClient;
use lethe_adapter_slack::slack::mapper::SlackAdapter;
use lethe_api::api::envelope::{ProjectionMetadata, ResponseEnvelope};
use lethe_api::api::grep::{GrepRecord, GrepRequest, PreparedGrepQuery};
use lethe_api::api::health::{DependencyHealthInfo, HealthResponse, LastSyncHealth, SyncMetrics};
use lethe_api::api::pagination::{
    KeysetCursorError, KeysetPage, PaginatedResponse, PaginationParams, decode_keyset_cursor,
    encode_keyset_cursor, paginate,
};
use lethe_api::api::read_mode::{ReadModeError, ReadModeResolver};
use lethe_core::domain::{
    ActorRef, AuthorityModel, BlobRef, CaptureModel, EntityRef, FailureClass, IngestResult,
    MAX_CLOCK_SKEW, Observation, ObservationId, ObserverRef, ProjectionHealth, ProjectionRef,
    ProjectionStatus, ReadMode, SchemaRef, SemVer, SourceSystemRef, SupplementalId,
    SupplementalRecord, consent_decision_from_observation, consent_decision_keys,
    consent_decision_order as shared_consent_decision_order,
};
use lethe_derivation_gemini::{GeminiSlideAnalyzer, SlideAnalysisProjector};
use lethe_engine::identity::projector::IdentityProjector;
use lethe_engine::identity::state::{IdentityApplyResult, IdentityNodeId, IdentityState};
use lethe_engine::identity::types::{
    IdentifierKey, IdentifierType, IdentityResolutionOutput, PersonCandidate, ResolvedPerson,
};
#[cfg(test)]
use lethe_engine::lake::LakeStore;
use lethe_engine::lake::{
    BlobStore, ConsentDecisionResolver, IngestRequest, ObservationPreparer,
    count_surplus_payload_fields,
};
use lethe_engine::projection::catalog::ProjectionCatalog;
use lethe_engine::projection::lineage::{LineageManifest, SourceSnapshot};
use lethe_engine::supplemental::SupplementalStore;
use lethe_history::{
    HistoryError, HistoryImportCommand, HistoryImportResult, HistoryInventoryReport,
    HistoryInventoryRequest, HistoryProjection, HistoryQueryRequest, HistoryQueryResponse,
};
use lethe_policy::governance::engine::PolicyEngine;
use lethe_policy::governance::filter::FilteringGate;
use lethe_policy::governance::types::{
    AccessScope, AuditEvent, AuditEventKind, ConsentStatus, Environment, MaskStrategy, Operation,
    PolicyOutcome, PolicyRequest, RestrictedFieldSpec, Role,
};
use lethe_profile_model::{
    GalleryImage, ImageCoordinates, ProfilePic, SlideAnalysisResult, StudentProfile,
};
use lethe_projection_answer_log::{AnswerLogProjector, AnswerLogRecord};
use lethe_projection_claim_queue::{ClaimQueueProjection, ClaimQueueProjector};
use lethe_projection_cognition::{
    CardQueueProjection, CardQueueProjector, CardQueueReducer, CognitionStateProjector,
    CommunicationProjectionState, FreshnessProjection, FreshnessProjector, FreshnessStatus,
    FreshnessThreshold, PlanStateProjection, ReplyLatency, ReplySloJoinIndex, ReplySloProjection,
    ReplySloStatus, ResumeSnapshotProjection, SourceFreshness,
};
use lethe_projection_corpus::{CorpusProjector, PrivacyFilter};
use lethe_projection_person::person_page::projector::PersonPageProjector;
use lethe_projection_person::person_page::types::{
    FrontendProfile, IdentityInfo, PersonActivity, PersonDetailResponse, PersonListItem,
    PersonMessage, PersonPageOutput, PersonProfile, PersonSlide, TimelineEvent,
};
use lethe_storage_api::{
    AppendOutcome as DurableAppendOutcome, AuditEventRecord, CutoverApiVersion, CutoverFixture,
    CutoverHealth, CutoverInventoryItem, CutoverReadinessReport, CutoverState,
    DiscoveredSlackThread, ObservationStats, OperationalAppendOutcome, OperationalAppendRequest,
    OperationalEventFilter, OperationalEventStats, OperationalStoragePorts, PersistedSyncState,
    ProjectionItem, ProjectionItemCommit, SlackThreadCatalogEntry, SlackThreadKey, StorageError,
    StoragePorts, StoredObservation, StoredOperationalEvent,
};
use lethe_storage_postgres::PostgresOperationalEventStore;
use lethe_storage_sqlite::persistence::{
    PersistenceError, SqliteOperationalEventStore, SqlitePersistence,
};

#[derive(Debug, thiserror::Error)]
pub enum SelfHostError {
    #[error(transparent)]
    Config(#[from] crate::self_host::config::ConfigError),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    SearchIndex(#[from] lethe_search_index::IndexError),
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error(transparent)]
    History(#[from] HistoryError),
    #[error("read mode error: {0}")]
    ReadMode(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("policy denied: {0}")]
    Policy(String),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("bulk import session conflict ({code}): {detail}")]
    BulkImportSessionConflict { code: &'static str, detail: String },
    #[error("import concurrency limit reached (maximum {maximum})")]
    ImportConcurrencyLimit { maximum: usize },
    #[error("projection stale: {0}")]
    ProjectionStale(String),
    #[error("{code}: {detail}")]
    SearchIndexUnavailable { code: &'static str, detail: String },
    #[error("supplemental validation failed: {code}")]
    SupplementalValidation {
        code: &'static str,
        detail: serde_json::Value,
    },
    #[error("supplemental conflict: {code}")]
    SupplementalConflict {
        code: &'static str,
        detail: serde_json::Value,
    },
    #[error("internal state lock poisoned")]
    LockPoisoned,
    #[error("ingestion rejected: {0}")]
    Ingestion(String),
    #[error("ingestion request rejected ({code}): {detail}")]
    IngestionRequest {
        code: &'static str,
        detail: String,
        details: serde_json::Value,
    },
    #[error("operational ledger startup failed: {0}")]
    OperationalLedger(String),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncReport {
    pub slack_ingested: usize,
    pub google_ingested: usize,
    pub slide_analyses: usize,
    pub duplicates: usize,
    pub quarantined: usize,
    pub dead_letters: Vec<DeadLetter>,
    pub last_sync_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ImportReport {
    pub ingested: usize,
    pub duplicates: usize,
    pub quarantined: usize,
    #[serde(default)]
    pub rejected: usize,
    #[serde(default)]
    pub results: Vec<ImportItemResult>,
    #[serde(default)]
    pub summary: ImportSummary,
}

#[derive(Debug)]
struct PreparedImportObservation {
    index: usize,
    client_ref: String,
    observation: Observation,
}

#[derive(Debug)]
struct V2IdentityError {
    code: &'static str,
    reason: String,
    details: Option<serde_json::Value>,
}

fn derive_v2_identity(
    mut draft: ObservationDraft,
    source_instance_id: &str,
) -> Result<ObservationDraft, V2IdentityError> {
    let meta = draft.meta.as_object().cloned().unwrap_or_default();
    let object_id = meta
        .get(OBJECT_ID_META_KEY)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| V2IdentityError {
            code: "identity_components_missing",
            reason: "meta.object_id is required for v2 ingestion".to_owned(),
            details: Some(serde_json::json!({"required": ["object_id", "canonical_json"]})),
        })?;
    let canonical_json = meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| V2IdentityError {
            code: "identity_components_missing",
            reason: "meta.canonical_json is required for v2 ingestion".to_owned(),
            details: Some(serde_json::json!({"required": ["object_id", "canonical_json"]})),
        })?;
    if serde_json::from_str::<serde_json::Value>(canonical_json).is_err() {
        return Err(V2IdentityError {
            code: "canonical_json_invalid",
            reason: "meta.canonical_json must contain valid JSON".to_owned(),
            details: None,
        });
    }

    let expected = identity_key(source_instance_id, object_id, canonical_json);
    if draft.idempotency_key != expected {
        return Err(V2IdentityError {
            code: "identity_mismatch",
            reason: "idempotency_key does not match the server-derived canonical identity"
                .to_owned(),
            details: Some(serde_json::json!({
                "expected_identity": expected.as_str(),
                "provided_identity": draft.idempotency_key.as_str(),
                "source_instance_id": source_instance_id,
                "object_id": object_id,
            })),
        });
    }

    let mut meta = meta;
    meta.insert(
        "source_instance".to_owned(),
        serde_json::Value::String(source_instance_id.to_owned()),
    );
    let container = meta
        .get("source_container")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("root")
        .to_owned();
    meta.insert(
        "source_container".to_owned(),
        serde_json::Value::String(format!("{source_instance_id}:{container}")),
    );
    draft.meta = serde_json::Value::Object(meta);
    Ok(draft)
}

fn rejected_item(
    client_ref: String,
    error_code: &'static str,
    reason: String,
    details: Option<serde_json::Value>,
) -> ImportItemResult {
    ImportItemResult {
        client_ref,
        outcome: ImportOutcome::Rejected,
        observation_id: None,
        existing_id: None,
        ticket: None,
        error_code: Some(error_code.to_owned()),
        failure_class: Some(ImportFailureClass::Validation),
        reason: Some(reason),
        details,
    }
}

fn transient_item(
    client_ref: String,
    reason: String,
    details: serde_json::Value,
) -> ImportItemResult {
    ImportItemResult {
        client_ref,
        outcome: ImportOutcome::Rejected,
        observation_id: None,
        existing_id: None,
        ticket: None,
        error_code: Some("transient_failure".to_owned()),
        failure_class: Some(ImportFailureClass::Transient),
        reason: Some(reason),
        details: Some(details),
    }
}

fn item_result_from_ingest_result(client_ref: String, result: IngestResult) -> ImportItemResult {
    match result {
        IngestResult::Ingested { id, .. } => ImportItemResult {
            client_ref,
            outcome: ImportOutcome::Ingested,
            observation_id: Some(id),
            existing_id: None,
            ticket: None,
            error_code: None,
            failure_class: None,
            reason: None,
            details: None,
        },
        IngestResult::Duplicate { existing_id } => ImportItemResult {
            client_ref,
            outcome: ImportOutcome::Duplicate,
            observation_id: None,
            existing_id: Some(existing_id),
            ticket: None,
            error_code: None,
            failure_class: None,
            reason: None,
            details: None,
        },
        IngestResult::Rejected { class, message } => ImportItemResult {
            client_ref,
            outcome: ImportOutcome::Rejected,
            observation_id: None,
            existing_id: None,
            ticket: None,
            error_code: Some(error_code_for_failure(class).to_owned()),
            failure_class: Some(import_failure_class(class)),
            reason: Some(message),
            details: None,
        },
        IngestResult::Quarantined { ticket } => {
            let error_code = error_code_for_quarantine(ticket.kind);
            ImportItemResult {
                client_ref,
                outcome: ImportOutcome::Quarantined,
                observation_id: None,
                existing_id: None,
                ticket: Some(ImportTicket {
                    id: ticket.id,
                    reason: ticket.reason.clone(),
                }),
                error_code: Some(error_code.to_owned()),
                failure_class: Some(ImportFailureClass::Quarantine),
                reason: Some(ticket.reason),
                details: matches!(
                    ticket.kind,
                    lethe_core::domain::QuarantineKind::ClockSkewFuture
                )
                .then(|| {
                    serde_json::json!({
                        "max_clock_skew_seconds": MAX_CLOCK_SKEW.num_seconds(),
                    })
                }),
            }
        }
    }
}

fn error_code_for_quarantine(kind: lethe_core::domain::QuarantineKind) -> &'static str {
    match kind {
        lethe_core::domain::QuarantineKind::Policy => "policy_quarantine",
        lethe_core::domain::QuarantineKind::ClockSkewFuture => "clock_skew_future",
        lethe_core::domain::QuarantineKind::CanonicalCollision => "canonical_collision",
        lethe_core::domain::QuarantineKind::Channel => "quarantine_required",
    }
}

fn import_failure_class(class: FailureClass) -> ImportFailureClass {
    match class {
        FailureClass::RetryableEffectFailure => ImportFailureClass::Transient,
        FailureClass::QuarantineFailure => ImportFailureClass::Quarantine,
        FailureClass::ValidationFailure
        | FailureClass::PolicyFailure
        | FailureClass::ConflictFailure
        | FailureClass::DeterminismFailure
        | FailureClass::NonRetryableEffectFailure => ImportFailureClass::Validation,
    }
}

fn error_code_for_failure(class: FailureClass) -> &'static str {
    match class {
        FailureClass::RetryableEffectFailure => "transient_failure",
        FailureClass::ValidationFailure => "schema_validation",
        FailureClass::PolicyFailure => "policy_validation",
        FailureClass::ConflictFailure => "identity_conflict",
        FailureClass::DeterminismFailure => "determinism_failure",
        FailureClass::NonRetryableEffectFailure => "non_retryable_failure",
        FailureClass::QuarantineFailure => "quarantine_required",
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ImportSummary {
    pub ingested: usize,
    pub duplicates: usize,
    pub quarantined: usize,
    pub rejected: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImportOutcome {
    Ingested,
    Duplicate,
    Quarantined,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImportFailureClass {
    Transient,
    Validation,
    Quarantine,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportTicket {
    pub id: String,
    pub reason: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportItemResult {
    pub client_ref: String,
    pub outcome: ImportOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation_id: Option<ObservationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_id: Option<ObservationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticket: Option<ImportTicket>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<ImportFailureClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ImportReport {
    fn refresh_summary(&mut self) {
        self.summary = ImportSummary {
            ingested: self.ingested,
            duplicates: self.duplicates,
            quarantined: self.quarantined,
            rejected: self.rejected,
        };
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeadLetter {
    pub source: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
struct SlideImageCandidate {
    object_id: String,
    content_url: String,
    center_x: f64,
    center_y: f64,
    z_index: usize,
    rotation_degrees: i32,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BreakGlassProjection {
    pub channels: Vec<BreakGlassChannel>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BreakGlassChannel {
    pub channel_id: String,
    pub kind: String,
    pub source_instance_id: String,
    pub external_id: String,
    pub channel_allowed: bool,
    pub senders: Vec<String>,
}

impl BreakGlassProjection {
    fn from_channels(channels: &[lethe_registry::registry::ChannelRecord]) -> Self {
        let mut channels = channels
            .iter()
            .filter(|channel| channel.enabled)
            .map(|channel| {
                let mut senders = channel.break_glass_senders.clone();
                senders.sort();
                BreakGlassChannel {
                    channel_id: channel.id.clone(),
                    kind: channel.kind.as_str().to_owned(),
                    source_instance_id: channel.source_instance_id.clone(),
                    external_id: channel.external_id.clone(),
                    channel_allowed: channel.break_glass_channel,
                    senders,
                }
            })
            .collect::<Vec<_>>();
        channels.sort_by(|left, right| left.channel_id.cmp(&right.channel_id));
        Self { channels }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionSnapshot {
    pub identity: IdentityResolutionOutput,
    pub person_page: PersonPageOutput,
    pub answer_log: Vec<AnswerLogRecord>,
    pub claim_queue: ClaimQueueProjection,
    pub freshness: FreshnessProjection,
    pub resume_snapshot: ResumeSnapshotProjection,
    pub plan_state: PlanStateProjection,
    pub card_queue: CardQueueProjection,
    pub reply_slo: ReplySloProjection,
    pub break_glass: BreakGlassProjection,
    pub built_at: DateTime<Utc>,
    pub lineage: LineageManifest,
}

// ReplyCard.agent_name is derived during the supplemental fold; rebuild older
// serialized snapshots so existing cards receive the attribution.
const NON_CORPUS_MATERIALIZATION_VERSION: u32 = 11;
const REPLY_SLO_ITEM_OWNER: &str = "__reply_slo__";
const CLAIM_QUEUE_ITEM_OWNER: &str = "__claim_queue__";
const CARD_QUEUE_ITEM_OWNER: &str = "__card_queue__";
const IDENTITY_EVENT_ITEM_OWNER: &str = "__identity_events__";
const PERSON_COMPONENT_ITEM_OWNER: &str = "__person_components__";
const NON_CORPUS_REBUILD_STAGING_PROJECTION_ID: &str = "proj:person-page:rebuild-staging";
const CANONICAL_OBSERVATION_FINGERPRINT_DOMAIN: &[u8] =
    b"lethe:canonical-observation-fingerprint:v1\0";
const SUPPLEMENTAL_FINGERPRINT_DOMAIN: &[u8] = b"lethe:supplemental-fingerprint:v2\0";
const IMPORT_PROCESS_BATCH_SIZE: usize = 512;
const COMMUNICATION_RECONSENT_PAGE_SIZE: usize = 512;
const OBSERVATION_IMPORT_SLOW_THRESHOLD_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NonCorpusDeltaKind {
    NoOp,
    FreshnessOnly,
    SlackMessage,
    Communication,
    DeclaredSchemaSkip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NonCorpusDeltaClassification {
    kind: NonCorpusDeltaKind,
}

impl NonCorpusDeltaClassification {
    fn materialization_mode(self) -> &'static str {
        match self.kind {
            NonCorpusDeltaKind::NoOp => "not_applicable",
            NonCorpusDeltaKind::FreshnessOnly
            | NonCorpusDeltaKind::SlackMessage
            | NonCorpusDeltaKind::Communication
            | NonCorpusDeltaKind::DeclaredSchemaSkip => "incremental",
        }
    }

    fn kind_as_str(self) -> &'static str {
        match self.kind {
            NonCorpusDeltaKind::NoOp => "no_op",
            NonCorpusDeltaKind::FreshnessOnly => "freshness_only",
            NonCorpusDeltaKind::SlackMessage => "slack_message",
            NonCorpusDeltaKind::Communication => "communication",
            NonCorpusDeltaKind::DeclaredSchemaSkip => "declared_schema_skip",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionFoldBehavior {
    FreshnessOnly,
    Communication,
    Incremental,
}

const PROJECTION_FOLD_DECLARATIONS: &[(&str, ProjectionFoldBehavior)] = &[
    (
        "schema:claude-message",
        ProjectionFoldBehavior::Communication,
    ),
    (
        "schema:chatgpt-message",
        ProjectionFoldBehavior::Communication,
    ),
    ("schema:github-event", ProjectionFoldBehavior::FreshnessOnly),
    (
        "schema:coding-agent-message",
        ProjectionFoldBehavior::Communication,
    ),
    (
        "schema:slack-message",
        ProjectionFoldBehavior::Communication,
    ),
    (
        "schema:gmail-message",
        ProjectionFoldBehavior::Communication,
    ),
    (
        "schema:discord-message",
        ProjectionFoldBehavior::Communication,
    ),
    (
        "schema:slack-channel-snapshot",
        ProjectionFoldBehavior::FreshnessOnly,
    ),
    (
        "schema:workspace-object-snapshot",
        ProjectionFoldBehavior::FreshnessOnly,
    ),
    (
        "schema:observer-heartbeat",
        ProjectionFoldBehavior::FreshnessOnly,
    ),
    (
        "schema:bot-answer-log",
        ProjectionFoldBehavior::FreshnessOnly,
    ),
    (
        "schema:slide-analysis-result",
        ProjectionFoldBehavior::Incremental,
    ),
    (
        "schema:consent-decision",
        ProjectionFoldBehavior::Incremental,
    ),
];

fn projection_fold_behavior(schema: &str) -> Option<ProjectionFoldBehavior> {
    PROJECTION_FOLD_DECLARATIONS
        .iter()
        .find_map(|(declared_schema, behavior)| (*declared_schema == schema).then_some(*behavior))
}

fn validate_projection_fold_declarations(
    registry: &lethe_registry::registry::RegistryStore,
    declarations: &[(&str, ProjectionFoldBehavior)],
) -> Result<(), SelfHostError> {
    let registered = registry
        .list_schemas()
        .into_iter()
        .map(|schema| schema.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let declared = declarations
        .iter()
        .map(|(schema, _)| (*schema).to_owned())
        .collect::<BTreeSet<_>>();
    if registered != declared {
        let missing = registered
            .difference(&declared)
            .cloned()
            .collect::<Vec<_>>();
        let extra = declared
            .difference(&registered)
            .cloned()
            .collect::<Vec<_>>();
        return Err(SelfHostError::Ingestion(format!(
            "projection fold declaration drift: missing={missing:?}, extra={extra:?}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportTimingStage {
    BulkOperationLockWait,
    PersistenceLockWait,
    SpawnBlockingWait,
    LedgerAppend,
    PublishClone,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ObservationImportTiming {
    bulk_operation_lock_wait_ms: u64,
    persistence_lock_wait_ms: u64,
    spawn_blocking_wait_ms: u64,
    ledger_append_ms: u64,
    app_core_clone_ms: u64,
    publish_clone_ms: u64,
    non_corpus_materialize_ms: u64,
    search_index_catch_up_ms: u64,
    audit_ms: u64,
    surplus_payload_fields: u64,
    total_ms: u64,
}

#[derive(Debug)]
struct ObservationImportTimer {
    started_at: Instant,
    timing: ObservationImportTiming,
}

impl ObservationImportTimer {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            timing: ObservationImportTiming::default(),
        }
    }

    fn record_stage(&mut self, stage: ImportTimingStage, elapsed: Duration) {
        let elapsed_ms = u64::try_from(elapsed.as_millis())
            .expect("observation import stage duration does not fit u64 milliseconds");
        match stage {
            ImportTimingStage::BulkOperationLockWait => {
                self.timing.bulk_operation_lock_wait_ms = self
                    .timing
                    .bulk_operation_lock_wait_ms
                    .saturating_add(elapsed_ms);
            }
            ImportTimingStage::PersistenceLockWait => {
                self.timing.persistence_lock_wait_ms = self
                    .timing
                    .persistence_lock_wait_ms
                    .saturating_add(elapsed_ms);
            }
            ImportTimingStage::SpawnBlockingWait => {
                self.timing.spawn_blocking_wait_ms = elapsed_ms;
            }
            ImportTimingStage::LedgerAppend => self.timing.ledger_append_ms = elapsed_ms,
            ImportTimingStage::PublishClone => self.timing.publish_clone_ms = elapsed_ms,
        }
    }

    fn finish(mut self) -> ObservationImportTiming {
        self.timing.total_ms = u64::try_from(self.started_at.elapsed().as_millis())
            .expect("observation import total duration does not fit u64 milliseconds")
            .saturating_add(self.timing.spawn_blocking_wait_ms);
        self.timing
    }

    fn record_surplus_payload_fields(&mut self, count: usize) {
        self.timing.surplus_payload_fields = self
            .timing
            .surplus_payload_fields
            .saturating_add(u64::try_from(count).expect("surplus field count fits u64"));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservationImportContext {
    schema_names: Vec<String>,
    subject_kinds: Vec<String>,
}

impl ObservationImportContext {
    fn from_drafts(drafts: &[ObservationDraft]) -> Self {
        let schema_names = drafts
            .iter()
            .map(|draft| draft.schema.as_str().to_owned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let subject_kinds = drafts
            .iter()
            .map(|draft| {
                draft
                    .subject
                    .as_str()
                    .split_once(':')
                    .map_or("<invalid>", |(kind, _)| kind)
                    .to_owned()
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self {
            schema_names,
            subject_kinds,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportMaterializationState {
    NotRun,
    Deferred,
    Classified(NonCorpusDeltaClassification),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservationImportTimingLog {
    context: ObservationImportContext,
    source_instance_id: String,
    timing: ObservationImportTiming,
    materialization_state: ImportMaterializationState,
    bulk_session_requested: bool,
    result: &'static str,
    ingested: usize,
    duplicates: usize,
    quarantined: usize,
}

impl ObservationImportTimingLog {
    #[cfg(test)]
    fn field_names() -> &'static [&'static str] {
        &[
            "import_timing",
            "source_instance_id",
            "schema_names",
            "subject_kinds",
            "bulk_operation_lock_wait_ms",
            "persistence_lock_wait_ms",
            "spawn_blocking_wait_ms",
            "ledger_append_ms",
            "app_core_clone_ms",
            "publish_clone_ms",
            "non_corpus_materialize_ms",
            "non_corpus_materialize_mode",
            "non_corpus_classification",
            "full_rebuild_reason",
            "search_index_catch_up_ms",
            "audit_ms",
            "surplus_payload_fields",
            "total_ms",
            "slow_threshold_ms",
            "bulk_session_requested",
            "ingested",
            "duplicates",
            "quarantined",
            "result",
        ]
    }

    fn emit(self) {
        let (materialization_mode, classification, full_rebuild_reason) =
            match self.materialization_state {
                ImportMaterializationState::NotRun => ("not_run", "not_run", "not_applicable"),
                ImportMaterializationState::Deferred => {
                    ("deferred", "deferred_bulk_session", "bulk_session_deferred")
                }
                ImportMaterializationState::Classified(classification) => (
                    classification.materialization_mode(),
                    classification.kind_as_str(),
                    "not_applicable",
                ),
            };
        let slow = self.timing.total_ms > OBSERVATION_IMPORT_SLOW_THRESHOLD_MS;
        let schema_names = self.context.schema_names.join(",");
        let subject_kinds = self.context.subject_kinds.join(",");
        if slow {
            tracing::warn!(
                import_timing = true,
                source_instance_id = %self.source_instance_id,
                schema_names = %schema_names,
                subject_kinds = %subject_kinds,
                bulk_operation_lock_wait_ms = self.timing.bulk_operation_lock_wait_ms,
                persistence_lock_wait_ms = self.timing.persistence_lock_wait_ms,
                spawn_blocking_wait_ms = self.timing.spawn_blocking_wait_ms,
                ledger_append_ms = self.timing.ledger_append_ms,
                app_core_clone_ms = self.timing.app_core_clone_ms,
                publish_clone_ms = self.timing.publish_clone_ms,
                non_corpus_materialize_ms = self.timing.non_corpus_materialize_ms,
                non_corpus_materialize_mode = materialization_mode,
                non_corpus_classification = classification,
                full_rebuild_reason,
                search_index_catch_up_ms = self.timing.search_index_catch_up_ms,
                audit_ms = self.timing.audit_ms,
                surplus_payload_fields = self.timing.surplus_payload_fields,
                total_ms = self.timing.total_ms,
                slow_threshold_ms = OBSERVATION_IMPORT_SLOW_THRESHOLD_MS,
                bulk_session_requested = self.bulk_session_requested,
                ingested = self.ingested,
                duplicates = self.duplicates,
                quarantined = self.quarantined,
                result = self.result,
                "observation import timing exceeded threshold"
            );
        } else {
            tracing::info!(
                import_timing = true,
                source_instance_id = %self.source_instance_id,
                schema_names = %schema_names,
                subject_kinds = %subject_kinds,
                bulk_operation_lock_wait_ms = self.timing.bulk_operation_lock_wait_ms,
                persistence_lock_wait_ms = self.timing.persistence_lock_wait_ms,
                spawn_blocking_wait_ms = self.timing.spawn_blocking_wait_ms,
                ledger_append_ms = self.timing.ledger_append_ms,
                app_core_clone_ms = self.timing.app_core_clone_ms,
                publish_clone_ms = self.timing.publish_clone_ms,
                non_corpus_materialize_ms = self.timing.non_corpus_materialize_ms,
                non_corpus_materialize_mode = materialization_mode,
                non_corpus_classification = classification,
                full_rebuild_reason,
                search_index_catch_up_ms = self.timing.search_index_catch_up_ms,
                audit_ms = self.timing.audit_ms,
                surplus_payload_fields = self.timing.surplus_payload_fields,
                total_ms = self.timing.total_ms,
                slow_threshold_ms = OBSERVATION_IMPORT_SLOW_THRESHOLD_MS,
                bulk_session_requested = self.bulk_session_requested,
                ingested = self.ingested,
                duplicates = self.duplicates,
                quarantined = self.quarantined,
                result = self.result,
                "observation import timing"
            );
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactProjectionState {
    identity: IdentityState,
    observation_ids_by_node: Vec<Vec<String>>,
    nodes_by_observation_id: BTreeMap<String, BTreeSet<IdentityNodeId>>,
    nodes_by_identifier_value: BTreeMap<String, BTreeSet<IdentityNodeId>>,
    consent_by_subject: BTreeMap<String, CompactConsentDecision>,
    consent_by_identifier: BTreeMap<String, CompactConsentDecision>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactConsentDecision {
    observation_id: String,
    subject: String,
    identifier: Option<String>,
    status: Option<String>,
    published: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
}

impl ConsentDecisionResolver for CompactProjectionState {
    fn resolve(
        &self,
        subject: &EntityRef,
        identifiers: &[String],
        _consent_scope: Option<&str>,
    ) -> ConsentStatus {
        self.consent_by_subject
            .get(subject.as_str())
            .into_iter()
            .chain(
                identifiers
                    .iter()
                    .filter_map(|identifier| self.consent_by_identifier.get(identifier)),
            )
            .max_by(|left, right| consent_decision_order(left).cmp(&consent_decision_order(right)))
            .and_then(|decision| decision.status.as_deref())
            .and_then(compact_consent_status)
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityReplayEvent {
    append_seq: u64,
    observation_id: String,
    candidates: Vec<PersonCandidate>,
    consent_decision: Option<CompactConsentDecision>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonComponentAggregate {
    person: ResolvedPerson,
    consent: ConsentStatus,
    fact_weight: u64,
    active_channels: BTreeSet<String>,
    slide_blob_refs: BTreeSet<String>,
    frontend_profile_rank: Option<FrontendProfileRank>,
    frontend_profile: Option<FrontendProfile>,
    profile: Option<PersonProfile>,
    activity: Option<PersonActivity>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FrontendProfileRank {
    richness_score: usize,
    created_at: DateTime<Utc>,
    stable_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPersonMessage {
    node_id: IdentityNodeId,
    id: String,
    source_observation_id: String,
    channel: String,
    text: String,
    ts: DateTime<Utc>,
    thread_ts: Option<String>,
    has_attachments: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPersonSlide {
    node_id: IdentityNodeId,
    id: String,
    source_observation_id: String,
    document_id: String,
    title: String,
    role: String,
    last_seen_revision: Option<String>,
    slide_count: Option<u32>,
    thumbnail_ref: Option<String>,
    last_modified: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct PendingProjectionItemCommit {
    commit: ProjectionItemCommit,
}

struct CompactApplyResult {
    touched_nodes: BTreeSet<IdentityNodeId>,
    affected_person_ids: BTreeSet<String>,
}

trait ComponentProjectionLookup {
    fn stored_observation(
        &self,
        observation_id: &ObservationId,
    ) -> Result<Option<StoredObservation>, SelfHostError>;

    fn observations_for_privacy_key_page(
        &self,
        privacy_key: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError>;

    fn person_message_items(&self, owner_key: &str) -> Result<Vec<ProjectionItem>, SelfHostError>;
}

struct StorageComponentProjectionLookup<'a> {
    storage: &'a dyn StoragePorts,
}

impl ComponentProjectionLookup for StorageComponentProjectionLookup<'_> {
    fn stored_observation(
        &self,
        observation_id: &ObservationId,
    ) -> Result<Option<StoredObservation>, SelfHostError> {
        Ok(self.storage.observation_by_id(observation_id)?)
    }

    fn observations_for_privacy_key_page(
        &self,
        privacy_key: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError> {
        Ok(self
            .storage
            .observations_for_privacy_key_page(privacy_key, after_append_seq, limit)?)
    }

    fn person_message_items(&self, owner_key: &str) -> Result<Vec<ProjectionItem>, SelfHostError> {
        Ok(self
            .storage
            .projection_items_by_owner(&ProjectionRef::new("proj:person-page"), owner_key)?)
    }
}

#[derive(Debug)]
struct AppSupplementalRollback {
    id: SupplementalId,
    store: lethe_engine::supplemental::store::UpsertRollback,
    previous_record: Option<SupplementalRecord>,
    current_record: SupplementalRecord,
    previous_resident_fingerprint: String,
    previous_count: usize,
    previous_claim_queue_dirty: bool,
}

#[derive(Debug, Clone)]
struct SupplementalProjectionCache {
    records: Vec<SupplementalRecord>,
    cognition_records: Vec<SupplementalRecord>,
    frontend_records: Vec<SupplementalRecord>,
    card_queue: CardQueueReducer,
    reply_slo: ReplySloJoinIndex,
}

impl SupplementalProjectionCache {
    fn from_records(records: &[SupplementalRecord]) -> Self {
        let mut ordered = records.to_vec();
        ordered.sort_by(supplemental_record_order);
        Self {
            cognition_records: ordered
                .iter()
                .filter(|record| is_cognition_activity_kind(&record.kind))
                .cloned()
                .collect(),
            frontend_records: ordered
                .iter()
                .filter(|record| record.kind == "slide-analysis")
                .cloned()
                .collect(),
            card_queue: CardQueueReducer::from_records(&ordered),
            reply_slo: ReplySloJoinIndex::from_records(&ordered),
            records: ordered,
        }
    }

    fn replace(&mut self, previous: Option<&SupplementalRecord>, current: &SupplementalRecord) {
        if let Some(previous) = previous {
            remove_supplemental_record(&mut self.records, &previous.id);
            remove_supplemental_record(&mut self.cognition_records, &previous.id);
            remove_supplemental_record(&mut self.frontend_records, &previous.id);
            self.card_queue.remove_record(&previous.id);
            self.reply_slo.remove_record(&previous.id);
        }
        insert_supplemental_record(&mut self.records, current.clone());
        if is_cognition_activity_kind(&current.kind) {
            insert_supplemental_record(&mut self.cognition_records, current.clone());
        }
        if current.kind == "slide-analysis" {
            insert_supplemental_record(&mut self.frontend_records, current.clone());
        }
        self.card_queue.upsert_record(current.clone());
        self.reply_slo.upsert_record(current.clone());
    }

    fn rollback(&mut self, current: &SupplementalRecord, previous: Option<&SupplementalRecord>) {
        remove_supplemental_record(&mut self.records, &current.id);
        remove_supplemental_record(&mut self.cognition_records, &current.id);
        remove_supplemental_record(&mut self.frontend_records, &current.id);
        self.card_queue.remove_record(&current.id);
        self.reply_slo.remove_record(&current.id);
        if let Some(previous) = previous {
            insert_supplemental_record(&mut self.records, previous.clone());
            if is_cognition_activity_kind(&previous.kind) {
                insert_supplemental_record(&mut self.cognition_records, previous.clone());
            }
            if previous.kind == "slide-analysis" {
                insert_supplemental_record(&mut self.frontend_records, previous.clone());
            }
            self.card_queue.upsert_record(previous.clone());
            self.reply_slo.upsert_record(previous.clone());
        }
    }

    fn claim_queue(&self) -> ClaimQueueProjection {
        ClaimQueueProjector.project_ordered_records(&self.records)
    }

    fn cognition(
        &self,
        claim_queue: &ClaimQueueProjection,
        built_at: DateTime<Utc>,
    ) -> (ResumeSnapshotProjection, PlanStateProjection) {
        CognitionStateProjector::new(built_at)
            .project_with_claim_queue(&self.cognition_records, claim_queue)
    }

    fn count(&self) -> usize {
        self.records.len()
    }
}

fn supplemental_record_order(
    left: &SupplementalRecord,
    right: &SupplementalRecord,
) -> std::cmp::Ordering {
    left.created_at
        .cmp(&right.created_at)
        .then_with(|| left.id.as_str().cmp(right.id.as_str()))
}

fn insert_supplemental_record(records: &mut Vec<SupplementalRecord>, record: SupplementalRecord) {
    let position = records
        .binary_search_by(|existing| supplemental_record_order(existing, &record))
        .unwrap_or_else(|position| position);
    records.insert(position, record);
}

fn remove_supplemental_record(records: &mut Vec<SupplementalRecord>, id: &SupplementalId) {
    if let Some(position) = records.iter().position(|record| &record.id == id) {
        records.remove(position);
    }
}

fn is_cognition_activity_kind(kind: &str) -> bool {
    matches!(kind, "session-summary@1" | "parking@1")
}

fn affects_claim_queue(kind: &str) -> bool {
    matches!(
        kind,
        "claim@1" | "claim-transition@1" | "verification-result@1" | "decision@1"
    )
}

type FrontendProfileSelections = BTreeMap<String, (usize, DateTime<Utc>, FrontendProfile)>;

#[derive(Debug, Clone)]
struct MaterializedProjectionSnapshot {
    format_version: u32,
    last_append_seq: u64,
    observation_count: u64,
    canonical_observation_fingerprint: String,
    supplemental_fingerprint: String,
    compact_state: CompactProjectionState,
    person_consents: BTreeMap<String, ConsentStatus>,
    person_components: BTreeMap<String, PersonComponentAggregate>,
    identity_event_count: u64,
    person_slide_count: u64,
    person_message_count: u64,
    reply_slo_count: u64,
    communication_projection: CommunicationProjectionState,
    snapshot: ProjectionSnapshot,
    pending_item_commit: Option<PendingProjectionItemCommit>,
}

#[derive(serde::Serialize)]
struct MaterializedProjectionManifestRef<'a> {
    format_version: u32,
    last_append_seq: u64,
    observation_count: u64,
    canonical_observation_fingerprint: &'a str,
    supplemental_fingerprint: &'a str,
    identity_event_count: u64,
    person_component_count: u64,
    person_slide_count: u64,
    person_message_count: u64,
    reply_slo_count: u64,
    communication_projection: &'a CommunicationProjectionState,
    snapshot: AuxiliaryProjectionSnapshotRef<'a>,
}

#[derive(serde::Serialize)]
struct AuxiliaryProjectionSnapshotRef<'a> {
    answer_log: &'a [AnswerLogRecord],
    claim_queue: &'a ClaimQueueProjection,
    freshness: &'a FreshnessProjection,
    resume_snapshot: &'a ResumeSnapshotProjection,
    plan_state: &'a PlanStateProjection,
    card_queue: &'a CardQueueProjection,
    break_glass: &'a BreakGlassProjection,
    built_at: DateTime<Utc>,
    lineage: &'a LineageManifest,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MaterializedProjectionManifest {
    format_version: u32,
    last_append_seq: u64,
    observation_count: u64,
    canonical_observation_fingerprint: String,
    supplemental_fingerprint: String,
    identity_event_count: u64,
    person_component_count: u64,
    person_slide_count: u64,
    person_message_count: u64,
    reply_slo_count: u64,
    #[serde(default)]
    communication_projection: CommunicationProjectionState,
    snapshot: AuxiliaryProjectionSnapshot,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AuxiliaryProjectionSnapshot {
    answer_log: Vec<AnswerLogRecord>,
    claim_queue: ClaimQueueProjection,
    freshness: FreshnessProjection,
    resume_snapshot: ResumeSnapshotProjection,
    plan_state: PlanStateProjection,
    card_queue: CardQueueProjection,
    break_glass: BreakGlassProjection,
    built_at: DateTime<Utc>,
    lineage: LineageManifest,
}

impl MaterializedProjectionSnapshot {
    fn manifest_value(&self) -> Result<serde_json::Value, SelfHostError> {
        let person_component_count = u64::try_from(self.person_components.len()).map_err(|_| {
            SelfHostError::Ingestion("person component count does not fit u64".to_owned())
        })?;
        Ok(serde_json::to_value(MaterializedProjectionManifestRef {
            format_version: self.format_version,
            last_append_seq: self.last_append_seq,
            observation_count: self.observation_count,
            canonical_observation_fingerprint: &self.canonical_observation_fingerprint,
            supplemental_fingerprint: &self.supplemental_fingerprint,
            identity_event_count: self.identity_event_count,
            person_component_count,
            person_slide_count: self.person_slide_count,
            person_message_count: self.person_message_count,
            reply_slo_count: self.reply_slo_count,
            communication_projection: &self.communication_projection,
            snapshot: AuxiliaryProjectionSnapshotRef {
                answer_log: &self.snapshot.answer_log,
                claim_queue: &self.snapshot.claim_queue,
                freshness: &self.snapshot.freshness,
                resume_snapshot: &self.snapshot.resume_snapshot,
                plan_state: &self.snapshot.plan_state,
                card_queue: &self.snapshot.card_queue,
                break_glass: &self.snapshot.break_glass,
                built_at: self.snapshot.built_at,
                lineage: &self.snapshot.lineage,
            },
        })?)
    }

    fn observation_stats(&self) -> ObservationStats {
        ObservationStats {
            count: self.observation_count,
            max_append_seq: self.last_append_seq,
        }
    }

    fn validate(&self) -> Result<(), SelfHostError> {
        decode_canonical_observation_fingerprint(&self.canonical_observation_fingerprint)?;
        decode_supplemental_fingerprint(&self.supplemental_fingerprint)?;
        if !self.snapshot.person_page.messages.is_empty() {
            return Err(SelfHostError::Ingestion(
                "proj:person-page manifest must not contain resident person messages".to_owned(),
            ));
        }
        let activity_message_count =
            person_message_activity_count(&self.snapshot.person_page.activities)?;
        if activity_message_count != self.person_message_count {
            return Err(SelfHostError::Ingestion(format!(
                "proj:person-page activity total_messages sum {activity_message_count} does not match manifest person_message_count {}",
                self.person_message_count
            )));
        }
        if !self.snapshot.reply_slo.rows.is_empty() || !self.snapshot.reply_slo.overdue.is_empty() {
            return Err(SelfHostError::Ingestion(
                "proj:person-page manifest must not contain resident reply SLO rows".to_owned(),
            ));
        }
        if self.communication_projection.len()
            != usize::try_from(self.reply_slo_count).map_err(|_| {
                SelfHostError::Ingestion("reply SLO count does not fit usize".to_owned())
            })?
        {
            return Err(SelfHostError::Ingestion(
                "communication projection fact count does not match reply SLO count".to_owned(),
            ));
        }
        if let Some(pending) = &self.pending_item_commit {
            validate_pending_projection_item_commit(
                pending,
                &self.compact_state,
                self.person_message_count,
                self.reply_slo_count,
                &self.snapshot.person_page.activities,
            )?;
        }
        self.compact_state.validate()?;
        let expected_identity = self.compact_state.resolve_identity();
        if serde_json::to_value(&expected_identity)?
            != serde_json::to_value(&self.snapshot.identity)?
        {
            return Err(SelfHostError::Ingestion(
                "proj:person-page identity output does not match compact identity state".to_owned(),
            ));
        }
        if self.compact_state.person_consents(&expected_identity) != self.person_consents {
            return Err(SelfHostError::Ingestion(
                "proj:person-page consent map does not match compact consent state".to_owned(),
            ));
        }
        let expected_build_id = person_page_build_id(
            &self.canonical_observation_fingerprint,
            self.observation_count,
            &self.supplemental_fingerprint,
        );
        if self.snapshot.lineage.build_id != expected_build_id {
            return Err(SelfHostError::Ingestion(format!(
                "proj:person-page materialization lineage build_id {} does not match canonical fingerprint",
                self.snapshot.lineage.build_id
            )));
        }
        if !self.snapshot.lineage.input_refs.is_empty() {
            return Err(SelfHostError::Ingestion(
                "proj:person-page materialization must not retain canonical input refs".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for ProjectionSnapshot {
    fn default() -> Self {
        Self {
            identity: IdentityResolutionOutput::default(),
            person_page: PersonPageOutput::default(),
            answer_log: Vec::new(),
            claim_queue: ClaimQueueProjection::default(),
            freshness: FreshnessProjection::default(),
            resume_snapshot: ResumeSnapshotProjection {
                projects: Vec::new(),
            },
            plan_state: PlanStateProjection {
                projects: Vec::new(),
            },
            card_queue: CardQueueProjection::default(),
            reply_slo: ReplySloProjection::default(),
            break_glass: BreakGlassProjection::default(),
            built_at: Utc::now(),
            lineage: LineageManifest::new(
                ProjectionRef::new("proj:person-page"),
                SemVer::new("1.0.0"),
                "build-uninitialized".to_string(),
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppCore {
    pub registry: lethe_registry::registry::RegistryStore,
    pub catalog: ProjectionCatalog,
    pub blobs: BlobStore,
    pub supplemental: SupplementalStore,
    freshness_thresholds: Vec<FreshnessThreshold>,
    observation_stats: ObservationStats,
    canonical_observation_fingerprint: String,
    supplemental_fingerprint: String,
    resident_supplemental_fingerprint: String,
    supplemental_count: usize,
    supplemental_projection_cache: SupplementalProjectionCache,
    claim_queue_dirty: bool,
    compact_state: CompactProjectionState,
    person_consents: BTreeMap<String, ConsentStatus>,
    person_components: BTreeMap<String, PersonComponentAggregate>,
    identity_event_count: u64,
    person_slide_count: u64,
    person_message_count: u64,
    reply_slo_count: u64,
    communication_projection: CommunicationProjectionState,
    pub snapshot: ProjectionSnapshot,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub last_sync_error: Option<String>,
    pub sync_metrics: SyncMetrics,
}

impl AppCore {
    fn manifest_value(&self) -> Result<serde_json::Value, SelfHostError> {
        let person_component_count = u64::try_from(self.person_components.len()).map_err(|_| {
            SelfHostError::Ingestion("person component count does not fit u64".to_owned())
        })?;
        Ok(serde_json::to_value(MaterializedProjectionManifestRef {
            format_version: NON_CORPUS_MATERIALIZATION_VERSION,
            last_append_seq: self.observation_stats.max_append_seq,
            observation_count: self.observation_stats.count,
            canonical_observation_fingerprint: &self.canonical_observation_fingerprint,
            supplemental_fingerprint: &self.supplemental_fingerprint,
            identity_event_count: self.identity_event_count,
            person_component_count,
            person_slide_count: self.person_slide_count,
            person_message_count: self.person_message_count,
            reply_slo_count: self.reply_slo_count,
            communication_projection: &self.communication_projection,
            snapshot: AuxiliaryProjectionSnapshotRef {
                answer_log: &self.snapshot.answer_log,
                claim_queue: &self.snapshot.claim_queue,
                freshness: &self.snapshot.freshness,
                resume_snapshot: &self.snapshot.resume_snapshot,
                plan_state: &self.snapshot.plan_state,
                card_queue: &self.snapshot.card_queue,
                break_glass: &self.snapshot.break_glass,
                built_at: self.snapshot.built_at,
                lineage: &self.snapshot.lineage,
            },
        })?)
    }

    #[cfg(test)]
    fn from_materialized(
        materialized: MaterializedProjectionSnapshot,
        persisted_blobs: Vec<Vec<u8>>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
    ) -> Result<Self, SelfHostError> {
        Self::from_materialized_with_sync_state(
            materialized,
            persisted_blobs,
            persisted_supplementals,
            freshness_thresholds,
            channels,
            None,
        )
    }

    fn from_materialized_with_sync_state(
        mut materialized: MaterializedProjectionSnapshot,
        persisted_blobs: Vec<Vec<u8>>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
        persisted_sync_state: Option<PersistedSyncState>,
    ) -> Result<Self, SelfHostError> {
        materialized.validate()?;
        let supplemental_projection_cache =
            SupplementalProjectionCache::from_records(&persisted_supplementals);
        let cached_claim_queue = supplemental_projection_cache.claim_queue();
        let (cached_resume_snapshot, cached_plan_state) = supplemental_projection_cache
            .cognition(&cached_claim_queue, materialized.snapshot.built_at);
        let cached_card_queue = supplemental_projection_cache
            .card_queue
            .projection(materialized.snapshot.built_at);
        for (name, persisted, cached) in [
            (
                "claim queue",
                serde_json::to_value(&materialized.snapshot.claim_queue)?,
                serde_json::to_value(&cached_claim_queue)?,
            ),
            (
                "resume snapshot",
                serde_json::to_value(&materialized.snapshot.resume_snapshot)?,
                serde_json::to_value(&cached_resume_snapshot)?,
            ),
            (
                "plan state",
                serde_json::to_value(&materialized.snapshot.plan_state)?,
                serde_json::to_value(&cached_plan_state)?,
            ),
            (
                "card queue",
                serde_json::to_value(&materialized.snapshot.card_queue)?,
                serde_json::to_value(&cached_card_queue)?,
            ),
        ] {
            if persisted != cached {
                return Err(SelfHostError::Ingestion(format!(
                    "proj:person-page persisted {name} diverged from supplemental reducer replay"
                )));
            }
        }
        materialized.snapshot.claim_queue = cached_claim_queue;
        materialized.snapshot.resume_snapshot = cached_resume_snapshot;
        materialized.snapshot.plan_state = cached_plan_state;
        materialized.snapshot.card_queue = cached_card_queue;

        let mut blobs = BlobStore::new();
        for blob in persisted_blobs {
            blobs.put(&blob);
        }

        let mut supplemental = SupplementalStore::new();
        let mut loaded_supplemental_ids = HashSet::new();
        for record in persisted_supplementals {
            let id = record.id.clone();
            supplemental
                .upsert_checked(
                    record,
                    |_| true,
                    |supplemental_id| loaded_supplemental_ids.contains(supplemental_id),
                )
                .map_err(|err| {
                    SelfHostError::Ingestion(format!(
                        "invalid persisted supplemental detected during bootstrap: {err}"
                    ))
                })?;
            loaded_supplemental_ids.insert(id);
        }

        let mut registry = seed_registry();
        for channel in channels {
            registry.register_channel(channel).map_err(|err| {
                SelfHostError::Config(crate::self_host::config::ConfigError::Invalid(
                    err.to_string(),
                ))
            })?;
        }
        validate_projection_fold_declarations(&registry, PROJECTION_FOLD_DECLARATIONS)?;

        let resident_supplemental_fingerprint = materialized.supplemental_fingerprint.clone();
        let supplemental_count = supplemental_projection_cache.count();
        materialized.snapshot.identity = IdentityResolutionOutput::default();
        materialized.snapshot.person_page = PersonPageOutput::default();
        let (last_sync_at, last_sync_error, sync_metrics) = match persisted_sync_state {
            Some(state) => (
                Some(state.completed_at),
                state.error,
                SyncMetrics {
                    fetched: state.metrics.fetched,
                    ingested: state.metrics.ingested,
                    skipped: state.metrics.skipped,
                    failed: state.metrics.failed,
                    quarantined: state.metrics.quarantined,
                    latency_ms: state.metrics.latency_ms,
                },
            ),
            None => (
                None,
                Some("persisted sync_metrics row for source all is missing".to_owned()),
                SyncMetrics::default(),
            ),
        };
        let mut core = Self {
            registry,
            catalog: seed_projection_catalog(),
            blobs,
            supplemental,
            freshness_thresholds,
            observation_stats: materialized.observation_stats(),
            canonical_observation_fingerprint: materialized.canonical_observation_fingerprint,
            supplemental_fingerprint: materialized.supplemental_fingerprint,
            resident_supplemental_fingerprint,
            supplemental_count,
            supplemental_projection_cache,
            claim_queue_dirty: false,
            compact_state: materialized.compact_state,
            person_consents: materialized.person_consents,
            person_components: materialized.person_components,
            identity_event_count: materialized.identity_event_count,
            person_slide_count: materialized.person_slide_count,
            person_message_count: materialized.person_message_count,
            reply_slo_count: materialized.reply_slo_count,
            communication_projection: materialized.communication_projection,
            snapshot: materialized.snapshot,
            last_sync_at,
            last_sync_error,
            sync_metrics,
        };
        core.activate_projections();
        Ok(core)
    }

    fn install_materialized(&mut self, mut materialized: MaterializedProjectionSnapshot) {
        materialized.snapshot.identity = IdentityResolutionOutput::default();
        materialized.snapshot.person_page = PersonPageOutput::default();
        self.observation_stats = materialized.observation_stats();
        self.canonical_observation_fingerprint = materialized.canonical_observation_fingerprint;
        self.supplemental_fingerprint = materialized.supplemental_fingerprint;
        self.resident_supplemental_fingerprint = self.supplemental_fingerprint.clone();
        self.supplemental_count = self.supplemental_projection_cache.count();
        self.claim_queue_dirty = false;
        self.compact_state = materialized.compact_state;
        self.person_consents = materialized.person_consents;
        self.person_components = materialized.person_components;
        self.identity_event_count = materialized.identity_event_count;
        self.person_slide_count = materialized.person_slide_count;
        self.person_message_count = materialized.person_message_count;
        self.reply_slo_count = materialized.reply_slo_count;
        self.communication_projection = materialized.communication_projection;
        self.snapshot = materialized.snapshot;
        self.activate_non_corpus_projections();
    }

    fn mark_non_corpus_materializations_stale(&mut self) {
        for projection_id in [
            "proj:identity-resolution",
            "proj:person-page",
            "proj:answer-log",
            "proj:claim-queue",
            "proj:freshness",
            "proj:resume-snapshot",
            "proj:plan-state",
            "proj:card-queue",
            "proj:reply-slo",
            "proj:break-glass",
        ] {
            let projection_ref = ProjectionRef::new(projection_id);
            self.catalog
                .set_status(&projection_ref, ProjectionStatus::Stale);
            self.catalog
                .set_health(&projection_ref, ProjectionHealth::Stale);
        }
    }

    #[cfg(test)]
    fn new(
        observations: Vec<Observation>,
        persisted_blobs: Vec<Vec<u8>>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
    ) -> Result<Self, SelfHostError> {
        Self::new_with_config(
            observations,
            persisted_blobs,
            persisted_supplementals,
            Vec::new(),
            Vec::new(),
        )
    }

    #[cfg(test)]
    fn new_with_config(
        observations: Vec<Observation>,
        persisted_blobs: Vec<Vec<u8>>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
    ) -> Result<Self, SelfHostError> {
        let observation_count = u64::try_from(observations.len()).map_err(|_| {
            SelfHostError::Ingestion("observation count does not fit u64".to_owned())
        })?;
        let materialized = MaterializedProjectionSnapshot::build(
            observations,
            persisted_supplementals.clone(),
            freshness_thresholds.clone(),
            channels.clone(),
            ObservationStats {
                count: observation_count,
                max_append_seq: observation_count,
            },
        )?;
        Self::from_materialized(
            materialized,
            persisted_blobs,
            persisted_supplementals,
            freshness_thresholds,
            channels,
        )
    }

    fn activate_projections(&mut self) {
        self.activate_non_corpus_projections();
        self.catalog
            .set_status(&ProjectionRef::new("proj:corpus"), ProjectionStatus::Active);
    }

    fn activate_non_corpus_projections(&mut self) {
        for projection_id in [
            "proj:identity-resolution",
            "proj:person-page",
            "proj:answer-log",
            "proj:claim-queue",
            "proj:freshness",
            "proj:resume-snapshot",
            "proj:plan-state",
            "proj:card-queue",
            "proj:reply-slo",
            "proj:break-glass",
        ] {
            let projection_ref = ProjectionRef::new(projection_id);
            self.catalog
                .set_status(&projection_ref, ProjectionStatus::Active);
            self.catalog
                .set_health(&projection_ref, ProjectionHealth::Healthy);
        }
    }

    fn upsert_supplemental_checked<ObservationExists, SupplementalExists>(
        &mut self,
        record: lethe_core::domain::SupplementalRecord,
        observation_exists: ObservationExists,
        supplemental_exists: SupplementalExists,
    ) -> Result<AppSupplementalRollback, lethe_core::domain::DomainError>
    where
        ObservationExists: Fn(&lethe_core::domain::ObservationId) -> bool,
        SupplementalExists: Fn(&lethe_core::domain::SupplementalId) -> bool,
    {
        let previous_record = self.supplemental.get(&record.id).cloned();
        let previous_resident_fingerprint = self.resident_supplemental_fingerprint.clone();
        let previous_count = self.supplemental_count;
        let previous_claim_queue_dirty = self.claim_queue_dirty;
        let next_count = if previous_record.is_none() {
            previous_count.checked_add(1).ok_or_else(|| {
                lethe_core::domain::DomainError::Validation(
                    "supplemental count overflow during upsert".to_owned(),
                )
            })?
        } else {
            previous_count
        };
        let store = self.supplemental.upsert_with_rollback_checked(
            record,
            observation_exists,
            supplemental_exists,
        )?;
        let current_record = self
            .supplemental
            .get(&store.id)
            .cloned()
            .expect("successful supplemental upsert must install the record");
        let next_fingerprint = match supplemental_fingerprint_after_delta(
            &previous_resident_fingerprint,
            previous_record.as_ref(),
            &current_record,
        ) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                self.supplemental.rollback_upsert(store);
                return Err(lethe_core::domain::DomainError::Validation(format!(
                    "supplemental fingerprint update failed: {error}"
                )));
            }
        };
        let mut communication_observation_ids = BTreeSet::new();
        for record in [previous_record.as_ref(), Some(&current_record)]
            .into_iter()
            .flatten()
        {
            if let Some(observation_id) = self
                .supplemental_projection_cache
                .reply_slo
                .observation_id_for_record(record)
            {
                communication_observation_ids.insert(observation_id.as_str().to_owned());
            }
        }
        self.supplemental_projection_cache
            .replace(previous_record.as_ref(), &current_record);
        if let Some(observation_id) = self
            .supplemental_projection_cache
            .reply_slo
            .observation_id_for_record(&current_record)
        {
            communication_observation_ids.insert(observation_id.as_str().to_owned());
        }
        for observation_id in communication_observation_ids {
            let observation_id = ObservationId::new(observation_id);
            self.communication_projection.refresh_sent_at(
                &observation_id,
                self.supplemental_projection_cache
                    .reply_slo
                    .sent_at_for_observation(&observation_id),
            );
        }
        self.resident_supplemental_fingerprint = next_fingerprint;
        self.supplemental_count = next_count;
        self.claim_queue_dirty = previous_claim_queue_dirty
            || affects_claim_queue(&current_record.kind)
            || previous_record
                .as_ref()
                .is_some_and(|record| affects_claim_queue(&record.kind));
        Ok(AppSupplementalRollback {
            id: current_record.id.clone(),
            store,
            previous_record,
            current_record,
            previous_resident_fingerprint,
            previous_count,
            previous_claim_queue_dirty,
        })
    }

    fn rollback_supplemental(&mut self, rollback: AppSupplementalRollback) {
        let mut communication_observation_ids = BTreeSet::new();
        for record in [
            Some(&rollback.current_record),
            rollback.previous_record.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(observation_id) = self
                .supplemental_projection_cache
                .reply_slo
                .observation_id_for_record(record)
            {
                communication_observation_ids.insert(observation_id.as_str().to_owned());
            }
        }
        self.supplemental_projection_cache
            .rollback(&rollback.current_record, rollback.previous_record.as_ref());
        for observation_id in communication_observation_ids {
            let observation_id = ObservationId::new(observation_id);
            self.communication_projection.refresh_sent_at(
                &observation_id,
                self.supplemental_projection_cache
                    .reply_slo
                    .sent_at_for_observation(&observation_id),
            );
        }
        self.resident_supplemental_fingerprint = rollback.previous_resident_fingerprint;
        self.supplemental_count = rollback.previous_count;
        self.claim_queue_dirty = rollback.previous_claim_queue_dirty;
        self.supplemental.rollback_upsert(rollback.store);
    }
}

fn prepare_draft(core: &AppCore, draft: ObservationDraft) -> Result<Observation, IngestResult> {
    ObservationPreparer::new_with_consent_resolver(&core.registry, &core.blobs, &core.compact_state)
        .prepare(IngestRequest {
            schema: draft.schema,
            schema_version: draft.schema_version,
            observer: draft.observer,
            source_system: draft.source_system,
            authority_model: draft.authority_model,
            capture_model: draft.capture_model,
            subject: draft.subject,
            target: draft.target,
            payload: draft.payload,
            attachments: draft.attachments,
            published: draft.published,
            idempotency_key: draft.idempotency_key,
            meta: draft.meta,
        })
}

fn privacy_audit_details(observations: &[Observation]) -> Vec<serde_json::Value> {
    observations
        .iter()
        .map(|observation| {
            let (decision, rule) = if observation.meta.get("retracts").is_some() {
                ("shield", "typed retraction target is applied incrementally")
            } else {
                (
                    "allow",
                    "capture gate evaluated latest consent before append",
                )
            };
            serde_json::json!({
                "actor": "actor:self-host",
                "subject": observation.subject,
                "scope": observation
                    .consent
                    .as_ref()
                    .map(|scope| scope.as_str())
                    .unwrap_or("record"),
                "decision": decision,
                "rule": rule,
                "timestamp": Utc::now(),
                "observation_id": observation.id,
            })
        })
        .collect()
}

#[derive(Clone)]
pub struct AppService {
    core: Arc<Mutex<AppCore>>,
    core_snapshot: Arc<ArcSwap<AppCore>>,
    persistence: Arc<Mutex<Box<dyn StoragePorts>>>,
    persistence_read_pool: Vec<Arc<Mutex<Box<dyn StoragePorts>>>>,
    persistence_read_next: Arc<std::sync::atomic::AtomicUsize>,
    operational_ledger: Arc<Mutex<Box<dyn OperationalStoragePorts>>>,
    operational_ledger_read_pool: Vec<Arc<Mutex<Box<dyn OperationalStoragePorts>>>>,
    operational_ledger_read_next: Arc<std::sync::atomic::AtomicUsize>,
    history_projection: Arc<Mutex<HistoryProjection>>,
    derived_projection_lane: Arc<Mutex<()>>,
    bulk_import_operation: Arc<Mutex<()>>,
    import_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    search_index: search_index::SearchIndexManager,
    search_jobs: Arc<Mutex<BTreeMap<String, SearchJobRecord>>>,
    search_job_sequence: Arc<std::sync::atomic::AtomicU64>,
    search_job_queue: Arc<SearchJobQueue>,
    config: Arc<SelfHostConfig>,
    slack_sources: Vec<SlackSourceRuntime>,
    google_sources: Vec<GoogleSourceRuntime>,
    slide_analyzer: Option<GeminiSlideAnalyzer>,
    resilient_executor: Arc<ResilientExecutor>,
    append_consumer_in_flight: Arc<std::sync::atomic::AtomicBool>,
    append_consumer_error: Arc<Mutex<Option<String>>>,
    search_index_catch_up_in_flight: Arc<std::sync::atomic::AtomicBool>,
    non_corpus_rebuild_in_flight: Arc<std::sync::atomic::AtomicBool>,
    non_corpus_rebuild_error: Arc<Mutex<Option<String>>>,
    #[cfg(test)]
    non_corpus_rebuild_count: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    non_corpus_rebuild_reasons: Arc<Mutex<Vec<&'static str>>>,
    #[cfg(test)]
    non_corpus_rebuild_page_count: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    non_corpus_rebuild_page_delay: Option<Duration>,
    #[cfg(test)]
    publish_count: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    search_job_test_gate: Option<Arc<std::sync::Barrier>>,
    #[cfg(test)]
    search_job_test_fault: Option<SearchJobTestFault>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchJobStatus {
    pub job_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
struct SearchJobRecord {
    sequence: u64,
    status: String,
    result: Option<serde_json::Value>,
    error: Option<String>,
}

struct ImportPermit {
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for ImportPermit {
    fn drop(&mut self) {
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

struct SearchJobWork {
    service: AppService,
    job_id: String,
    request: GrepRequest,
}

struct SearchJobQueue {
    sender: SyncSender<SearchJobWork>,
}

impl SearchJobQueue {
    fn try_submit(&self, work: SearchJobWork) -> Result<(), ()> {
        match self.sender.try_send(work) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => Err(()),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
enum SearchJobTestFault {
    Error,
    Panic,
}

fn start_search_job_workers(worker_count: usize) -> Result<Arc<SearchJobQueue>, SelfHostError> {
    if worker_count == 0 {
        return Err(SelfHostError::ReadMode(
            "limits.max_search_job_workers must be positive".to_owned(),
        ));
    }
    let (sender, receiver) = mpsc::sync_channel(worker_count);
    let queue = Arc::new(SearchJobQueue { sender });
    let receiver = Arc::new(Mutex::new(receiver));
    for worker_index in 0..worker_count {
        let receiver = Arc::clone(&receiver);
        std::thread::Builder::new()
            .name(format!("lethe-search-job-{worker_index}"))
            .spawn(move || search_job_worker(receiver))
            .map_err(|error| {
                SelfHostError::ReadMode(format!(
                    "failed to spawn search job worker {worker_index}: {error}"
                ))
            })?;
    }
    Ok(queue)
}

fn search_job_worker(receiver: Arc<Mutex<Receiver<SearchJobWork>>>) {
    loop {
        let next = match receiver.lock() {
            Ok(receiver) => receiver.recv(),
            Err(error) => {
                tracing::error!(error = %error, "search job queue receiver lock poisoned");
                return;
            }
        };
        let work = match next {
            Ok(work) => work,
            Err(_) => return,
        };
        let service = work.service.clone();
        let job_id = work.job_id.clone();
        let outcome = catch_unwind(AssertUnwindSafe(|| service.run_search_job(work)));
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(error)) => service.mark_search_job_failed(&job_id, error.to_string()),
            Err(panic) => service.mark_search_job_failed(
                &job_id,
                format!("search job worker panicked: {}", panic_message(panic)),
            ),
        }
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

#[cfg(test)]
impl ProjectionSnapshot {
    pub fn build(
        observations: Vec<Observation>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
    ) -> Result<Self, SelfHostError> {
        let observation_count = u64::try_from(observations.len()).map_err(|_| {
            SelfHostError::Ingestion("observation count does not fit u64".to_owned())
        })?;
        Ok(Self::build_with_state(
            observations,
            persisted_supplementals,
            freshness_thresholds,
            channels,
            ObservationStats {
                count: observation_count,
                max_append_seq: observation_count,
            },
            Utc::now(),
        )?
        .snapshot)
    }

    fn build_with_state(
        observations: Vec<Observation>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
        stats: ObservationStats,
        built_at: DateTime<Utc>,
    ) -> Result<BuiltProjectionSnapshot, SelfHostError> {
        let observation_count = u64::try_from(observations.len()).map_err(|_| {
            SelfHostError::Ingestion("observation count does not fit u64".to_owned())
        })?;
        if observation_count != stats.count {
            return Err(SelfHostError::Ingestion(format!(
                "projection rebuild loaded {observation_count} observations, but canonical stats report {}",
                stats.count
            )));
        }
        let canonical_observation_fingerprint = canonical_observation_fingerprint(&observations)?;
        let supplemental_fingerprint = supplemental_fingerprint(&persisted_supplementals)?;
        let supplemental_count = persisted_supplementals.len();
        let mut lake = LakeStore::new();
        for observation in observations {
            lake.append(observation).map_err(|existing_id| {
                SelfHostError::Ingestion(format!(
                    "duplicate persisted observation detected during projection build: {existing_id}"
                ))
            })?;
        }
        let mut supplemental = SupplementalStore::new();
        for record in persisted_supplementals {
            supplemental.upsert(record, &lake).map_err(|error| {
                SelfHostError::Ingestion(format!(
                    "invalid persisted supplemental during projection build: {error}"
                ))
            })?;
        }
        let compact_state = CompactProjectionState::build(lake.list())?;
        let identity = compact_state.resolve_identity();
        let person_consents = compact_state.person_consents(&identity);
        let supplemental_records = supplemental.by_kind("slide-analysis");
        let all_supplemental_records = supplemental.list().into_iter().cloned().collect::<Vec<_>>();
        let person_page =
            PersonPageProjector::project(&identity, lake.list(), &supplemental_records);
        let answer_log = AnswerLogProjector.project_observations(lake.list());
        let claim_queue = ClaimQueueProjector.project_records(&all_supplemental_records);
        let freshness = FreshnessProjector::new(freshness_thresholds, built_at)
            .project_observations(lake.list());
        let cognition_projector = CognitionStateProjector::new(built_at);
        let (resume_snapshot, plan_state) =
            cognition_projector.project_with_claim_queue(&all_supplemental_records, &claim_queue);
        let card_queue =
            CardQueueProjector::new(built_at).project_records(&all_supplemental_records);
        let reply_slo_join_index = ReplySloJoinIndex::from_records(&all_supplemental_records);
        let communication_projection =
            CommunicationProjectionState::from_observations(lake.list(), &reply_slo_join_index);
        let reply_slo = communication_projection.project(built_at);
        let break_glass = BreakGlassProjection::from_channels(&channels);
        let lineage = build_person_page_lineage(
            &canonical_observation_fingerprint,
            stats,
            &supplemental_fingerprint,
            supplemental_count,
            person_page.profiles.len()
                + person_page.slides.len()
                + person_page.messages.len()
                + person_page.activities.len(),
            built_at,
        );
        Ok(BuiltProjectionSnapshot {
            snapshot: Self {
                identity,
                person_page,
                answer_log,
                claim_queue,
                freshness,
                resume_snapshot,
                plan_state,
                card_queue,
                reply_slo,
                break_glass,
                built_at,
                lineage,
            },
            person_consents,
            canonical_observation_fingerprint,
            supplemental_fingerprint,
            compact_state,
            communication_projection,
        })
    }
}

#[cfg(test)]
struct BuiltProjectionSnapshot {
    snapshot: ProjectionSnapshot,
    person_consents: BTreeMap<String, ConsentStatus>,
    canonical_observation_fingerprint: String,
    supplemental_fingerprint: String,
    compact_state: CompactProjectionState,
    communication_projection: CommunicationProjectionState,
}

impl CompactProjectionState {
    fn build(observations: &[Observation]) -> Result<Self, SelfHostError> {
        let mut state = Self {
            identity: IdentityState::default(),
            observation_ids_by_node: Vec::new(),
            nodes_by_observation_id: BTreeMap::new(),
            nodes_by_identifier_value: BTreeMap::new(),
            consent_by_subject: BTreeMap::new(),
            consent_by_identifier: BTreeMap::new(),
        };
        for observation in observations {
            state.capture_consent_decision(observation);
            for candidate in
                IdentityProjector::extract_candidates(std::slice::from_ref(observation))
            {
                state.add_identity_candidate(candidate, observation.id.as_str())?;
            }
        }
        state.validate()?;
        Ok(state)
    }

    fn apply_observation_page(
        &mut self,
        observations: &[Observation],
    ) -> Result<CompactApplyResult, SelfHostError> {
        let mut touched_members = BTreeSet::new();
        let mut affected_person_ids = BTreeSet::new();
        for observation in observations {
            if let Some(decision) = compact_consent_decision_from_observation(observation) {
                let mut consent_nodes = self
                    .identity
                    .component_members_for_person(&decision.subject)
                    .into_iter()
                    .flatten()
                    .copied()
                    .collect::<BTreeSet<_>>();
                if let Some(identifier) = &decision.identifier
                    && let Some(nodes) = self.nodes_by_identifier_value.get(identifier)
                {
                    consent_nodes.extend(nodes);
                }
                for node_id in consent_nodes {
                    if let Some(person_id) = self.identity.person_id_for_node(node_id) {
                        affected_person_ids.insert(person_id);
                    }
                    touched_members.insert(node_id);
                }
                self.record_consent_decision(decision);
            }
            let candidates =
                IdentityProjector::extract_candidates(std::slice::from_ref(observation));
            if observation.schema.as_str() == "schema:slack-message" && candidates.len() != 1 {
                return Err(SelfHostError::Ingestion(format!(
                    "Slack observation {} must yield exactly one identity candidate for compact incremental materialization",
                    observation.id
                )));
            }
            for candidate in candidates {
                let applied = self.add_identity_candidate(candidate, observation.id.as_str())?;
                touched_members.insert(applied.node_id);
                affected_person_ids.extend(applied.affected_person_ids);
            }
        }
        self.validate_delta(&touched_members)?;
        Ok(CompactApplyResult {
            touched_nodes: touched_members,
            affected_person_ids,
        })
    }

    fn add_identity_candidate(
        &mut self,
        candidate: PersonCandidate,
        observation_id: &str,
    ) -> Result<IdentityApplyResult, SelfHostError> {
        let mut existing_node = None;
        if candidate.source == "slack" {
            let user_identifier = candidate
                .identifiers
                .iter()
                .find(|identifier| identifier.identifier_type == IdentifierType::UserId)
                .ok_or_else(|| {
                    SelfHostError::Ingestion(
                        "Slack identity candidate is missing its source user_id".to_owned(),
                    )
                })?;
            let key =
                IdentifierKey::from_identifier(user_identifier).map_err(identity_state_error)?;
            existing_node = self.identity.node_for_key(&key);
        }
        let applied = match existing_node {
            Some(node_id) => self
                .identity
                .apply_update(node_id, candidate)
                .map_err(identity_state_error)?,
            None => self
                .identity
                .apply_new(candidate)
                .map_err(identity_state_error)?,
        };
        let node_index = usize::try_from(applied.node_id).map_err(|_| {
            SelfHostError::Ingestion(format!(
                "identity node {} does not fit usize",
                applied.node_id
            ))
        })?;
        if node_index == self.observation_ids_by_node.len() {
            self.observation_ids_by_node.push(Vec::new());
        }
        let observation_ids = self
            .observation_ids_by_node
            .get_mut(node_index)
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "identity node {} has no observation index",
                    applied.node_id
                ))
            })?;
        if observation_ids.iter().any(|id| id == observation_id) {
            return Err(SelfHostError::Ingestion(format!(
                "identity node {} repeats observation {observation_id}",
                applied.node_id
            )));
        }
        observation_ids.push(observation_id.to_owned());
        self.nodes_by_observation_id
            .entry(observation_id.to_owned())
            .or_default()
            .insert(applied.node_id);
        for identifier in &self
            .identity
            .node(applied.node_id)
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "identity node {} disappeared after candidate apply",
                    applied.node_id
                ))
            })?
            .candidate
            .identifiers
        {
            self.nodes_by_identifier_value
                .entry(identifier.value.clone())
                .or_default()
                .insert(applied.node_id);
        }
        Ok(applied)
    }

    fn capture_consent_decision(&mut self, observation: &Observation) {
        if let Some(decision) = compact_consent_decision_from_observation(observation) {
            self.record_consent_decision(decision);
        }
    }

    fn record_consent_decision(&mut self, decision: CompactConsentDecision) {
        update_latest_consent(
            &mut self.consent_by_subject,
            decision.subject.clone(),
            decision.clone(),
        );
        if let Some(identifier) = &decision.identifier {
            update_latest_consent(
                &mut self.consent_by_identifier,
                identifier.clone(),
                decision,
            );
        }
    }

    fn apply_replay_event(&mut self, event: &IdentityReplayEvent) -> Result<(), SelfHostError> {
        if event.observation_id.trim().is_empty() || event.append_seq == 0 {
            return Err(SelfHostError::Ingestion(
                "identity replay event has blank provenance or zero append sequence".to_owned(),
            ));
        }
        if let Some(decision) = &event.consent_decision {
            if decision.observation_id != event.observation_id {
                return Err(SelfHostError::Ingestion(format!(
                    "identity replay event {} contains consent provenance {}",
                    event.observation_id, decision.observation_id
                )));
            }
            self.record_consent_decision(decision.clone());
        }
        for candidate in &event.candidates {
            self.add_identity_candidate(candidate.clone(), &event.observation_id)?;
        }
        Ok(())
    }

    fn resolve_identity(&self) -> IdentityResolutionOutput {
        self.identity.resolution("1.0.0")
    }

    fn fact_node(
        &self,
        observation_id: &str,
        person_id: &str,
    ) -> Result<IdentityNodeId, SelfHostError> {
        self.nodes_by_observation_id
            .get(observation_id)
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "person fact references identity-free observation {observation_id}"
                ))
            })?
            .iter()
            .copied()
            .find(|node_id| {
                self.identity.person_id_for_node(*node_id).as_deref() == Some(person_id)
            })
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "person fact observation {observation_id} has no identity node in {person_id}"
                ))
            })
    }

    fn person_id_for_node(&self, node_id: IdentityNodeId) -> Result<String, SelfHostError> {
        self.identity.person_id_for_node(node_id).ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "identity node {node_id} has no resolved public person ID"
            ))
        })
    }

    fn person_consents(
        &self,
        identity: &IdentityResolutionOutput,
    ) -> BTreeMap<String, ConsentStatus> {
        identity
            .resolved_persons
            .iter()
            .map(|person| {
                (
                    person.person_id.as_str().to_owned(),
                    self.person_consent(person),
                )
            })
            .collect()
    }

    fn person_consent(&self, person: &ResolvedPerson) -> ConsentStatus {
        let decision = self
            .consent_by_subject
            .get(person.person_id.as_str())
            .into_iter()
            .chain(
                person
                    .identifiers
                    .iter()
                    .filter_map(|identifier| self.consent_by_identifier.get(&identifier.value)),
            )
            .max_by(|left, right| consent_decision_order(left).cmp(&consent_decision_order(right)));
        decision
            .and_then(|decision| decision.status.as_deref())
            .and_then(compact_consent_status)
            .unwrap_or_default()
    }

    fn validate_delta(
        &self,
        touched_nodes: &BTreeSet<IdentityNodeId>,
    ) -> Result<(), SelfHostError> {
        for node_id in touched_nodes {
            let node_index = usize::try_from(*node_id).map_err(|_| {
                SelfHostError::Ingestion(format!("identity node {node_id} does not fit usize"))
            })?;
            let observation_ids =
                self.observation_ids_by_node
                    .get(node_index)
                    .ok_or_else(|| {
                        SelfHostError::Ingestion(format!(
                            "identity node {node_id} has no observation index"
                        ))
                    })?;
            if observation_ids.is_empty() {
                return Err(SelfHostError::Ingestion(format!(
                    "identity node {node_id} has no source observation"
                )));
            }
            for observation_id in observation_ids {
                if !self
                    .nodes_by_observation_id
                    .get(observation_id)
                    .is_some_and(|nodes| nodes.contains(node_id))
                {
                    return Err(SelfHostError::Ingestion(format!(
                        "identity observation reverse index is missing {observation_id}/{node_id}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), SelfHostError> {
        self.identity.validate().map_err(identity_state_error)?;
        if self.identity.nodes().len() != self.observation_ids_by_node.len() {
            return Err(SelfHostError::Ingestion(
                "identity nodes and observation indexes have different lengths".to_owned(),
            ));
        }
        for (node_id, observation_ids) in self.observation_ids_by_node.iter().enumerate() {
            if observation_ids.is_empty() {
                return Err(SelfHostError::Ingestion(format!(
                    "identity node {node_id} has no source observation"
                )));
            }
            let mut observation_ids = BTreeSet::new();
            for observation_id in &self.observation_ids_by_node[node_id] {
                if observation_id.trim().is_empty() {
                    return Err(SelfHostError::Ingestion(format!(
                        "identity node {node_id} has a blank source observation"
                    )));
                }
                if !observation_ids.insert(observation_id) {
                    return Err(SelfHostError::Ingestion(format!(
                        "identity node {node_id} repeats source observation {observation_id}"
                    )));
                }
            }
        }
        let indexed_observation_ids = self
            .observation_ids_by_node
            .iter()
            .enumerate()
            .flat_map(|(node_id, observation_ids)| {
                observation_ids.iter().map(move |observation_id| {
                    (
                        observation_id.clone(),
                        u64::try_from(node_id).expect("validated identity node index fits u64"),
                    )
                })
            })
            .collect::<BTreeSet<_>>();
        let reverse_observation_ids = self
            .nodes_by_observation_id
            .iter()
            .flat_map(|(observation_id, node_ids)| {
                node_ids
                    .iter()
                    .map(move |node_id| (observation_id.clone(), *node_id))
            })
            .collect::<BTreeSet<_>>();
        if indexed_observation_ids != reverse_observation_ids {
            return Err(SelfHostError::Ingestion(
                "identity observation forward and reverse indexes differ".to_owned(),
            ));
        }
        let indexed_identifier_nodes = self
            .identity
            .nodes()
            .iter()
            .flat_map(|node| {
                node.candidate
                    .identifiers
                    .iter()
                    .map(move |identifier| (identifier.value.clone(), node.node_id))
            })
            .collect::<BTreeSet<_>>();
        let reverse_identifier_nodes = self
            .nodes_by_identifier_value
            .iter()
            .flat_map(|(identifier, nodes)| {
                nodes
                    .iter()
                    .map(move |node_id| (identifier.clone(), *node_id))
            })
            .collect::<BTreeSet<_>>();
        if indexed_identifier_nodes != reverse_identifier_nodes {
            return Err(SelfHostError::Ingestion(
                "identity identifier-value forward and reverse indexes differ".to_owned(),
            ));
        }
        Ok(())
    }
}

fn consent_decision_order(
    decision: &CompactConsentDecision,
) -> (DateTime<Utc>, DateTime<Utc>, &str) {
    shared_consent_decision_order(
        decision.published,
        decision.recorded_at,
        decision.observation_id.as_str(),
    )
}

fn update_latest_consent(
    index: &mut BTreeMap<String, CompactConsentDecision>,
    key: String,
    decision: CompactConsentDecision,
) {
    match index.get(&key) {
        Some(current) if consent_decision_order(current) >= consent_decision_order(&decision) => {}
        _ => {
            index.insert(key, decision);
        }
    }
}

fn identity_state_error(error: String) -> SelfHostError {
    SelfHostError::Ingestion(format!("identity state invariant failed: {error}"))
}

fn compact_consent_decision_from_observation(
    observation: &Observation,
) -> Option<CompactConsentDecision> {
    (observation.schema.as_str()
        == lethe_projection_person::person_page::projector::CONSENT_DECISION_SCHEMA)
        .then(|| CompactConsentDecision {
            observation_id: observation.id.as_str().to_owned(),
            subject: observation.subject.as_str().to_owned(),
            identifier: observation
                .payload
                .get("identifier")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            status: observation
                .payload
                .get("status")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            published: observation.published,
            recorded_at: observation.recorded_at,
        })
}

fn person_component_aggregates(
    identity: &IdentityResolutionOutput,
    person_page: &PersonPageOutput,
    person_consents: &BTreeMap<String, ConsentStatus>,
) -> Result<BTreeMap<String, PersonComponentAggregate>, SelfHostError> {
    let mut profiles_by_person = BTreeMap::new();
    for profile in &person_page.profiles {
        if profiles_by_person
            .insert(profile.person_id.as_str().to_owned(), profile.clone())
            .is_some()
        {
            return Err(SelfHostError::Ingestion(format!(
                "duplicate person profile for {}",
                profile.person_id
            )));
        }
    }
    let mut activities_by_person = BTreeMap::new();
    for activity in &person_page.activities {
        if activities_by_person
            .insert(activity.person_id.as_str().to_owned(), activity.clone())
            .is_some()
        {
            return Err(SelfHostError::Ingestion(format!(
                "duplicate person activity for {}",
                activity.person_id
            )));
        }
    }
    let mut slide_blob_refs_by_person = BTreeMap::<String, BTreeSet<String>>::new();
    for slide in &person_page.slides {
        let refs = slide_blob_refs_by_person
            .entry(slide.person_id.as_str().to_owned())
            .or_default();
        if let Some(blob_ref) = &slide.thumbnail_ref {
            refs.insert(blob_ref.clone());
        }
    }

    let mut components = BTreeMap::new();
    for person in &identity.resolved_persons {
        let person_id = person.person_id.as_str();
        let consent = person_consents.get(person_id).cloned().unwrap_or_default();
        let profile = profiles_by_person.remove(person_id);
        let activity = activities_by_person.remove(person_id);
        let slide_blob_refs = slide_blob_refs_by_person
            .remove(person_id)
            .unwrap_or_default();
        if (profile.is_some() != activity.is_some())
            || (consent == ConsentStatus::OptedOut && profile.is_some())
            || (consent != ConsentStatus::OptedOut && profile.is_none())
        {
            return Err(SelfHostError::Ingestion(format!(
                "person component {person_id} profile/activity visibility is inconsistent with consent"
            )));
        }
        let frontend_profile_rank = match profile.as_ref().and_then(|profile| {
            profile
                .frontend_profile
                .as_ref()
                .map(|frontend| (profile, frontend))
        }) {
            Some((profile, frontend)) => Some(FrontendProfileRank {
                richness_score: frontend.profile.richness_score(),
                created_at: profile.frontend_profile_created_at.ok_or_else(|| {
                    SelfHostError::Ingestion(format!(
                        "person component {person_id} frontend profile has no created_at"
                    ))
                })?,
                stable_id: frontend.source_document_id.clone(),
            }),
            None => None,
        };
        let slide_blob_refs = if consent == ConsentStatus::OptedOut {
            BTreeSet::new()
        } else {
            slide_blob_refs
        };
        let component = PersonComponentAggregate {
            person: person.clone(),
            consent,
            fact_weight: activity
                .as_ref()
                .map(component_activity_fact_weight)
                .transpose()?
                .unwrap_or(0),
            active_channels: activity
                .as_ref()
                .map(|activity| activity.active_channels.iter().cloned().collect())
                .unwrap_or_default(),
            slide_blob_refs,
            frontend_profile_rank,
            frontend_profile: profile
                .as_ref()
                .and_then(|profile| profile.frontend_profile.clone()),
            profile,
            activity,
        };
        if components.insert(person_id.to_owned(), component).is_some() {
            return Err(SelfHostError::Ingestion(format!(
                "duplicate person component {person_id}"
            )));
        }
    }
    if let Some(person_id) = profiles_by_person
        .keys()
        .chain(activities_by_person.keys())
        .chain(slide_blob_refs_by_person.keys())
        .next()
    {
        return Err(SelfHostError::Ingestion(format!(
            "person-page row references unknown person {person_id}"
        )));
    }
    Ok(components)
}

#[cfg(test)]
fn identity_replay_event_count(observations: &[Observation]) -> Result<u64, SelfHostError> {
    let count = observations
        .iter()
        .filter(|observation| {
            observation.schema.as_str()
                == lethe_projection_person::person_page::projector::CONSENT_DECISION_SCHEMA
                || !IdentityProjector::extract_candidates(std::slice::from_ref(observation))
                    .is_empty()
        })
        .count();
    u64::try_from(count)
        .map_err(|_| SelfHostError::Ingestion("identity event count does not fit u64".to_owned()))
}

fn compact_consent_status(value: &str) -> Option<ConsentStatus> {
    match value {
        "unrestricted" => Some(ConsentStatus::Unrestricted),
        "restricted_capture" => Some(ConsentStatus::RestrictedCapture),
        "opted_out" => Some(ConsentStatus::OptedOut),
        _ => None,
    }
}

impl MaterializedProjectionSnapshot {
    #[cfg(test)]
    fn build(
        observations: Vec<Observation>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
        stats: ObservationStats,
    ) -> Result<Self, SelfHostError> {
        Self::build_at(
            observations,
            persisted_supplementals,
            freshness_thresholds,
            channels,
            stats,
            Utc::now(),
        )
    }

    #[cfg(test)]
    fn build_at(
        observations: Vec<Observation>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
        stats: ObservationStats,
        built_at: DateTime<Utc>,
    ) -> Result<Self, SelfHostError> {
        let fact_append_sequences = observation_append_sequences(&observations, stats)?;
        let identity_event_count = identity_replay_event_count(&observations)?;
        let identity_event_items = observations
            .iter()
            .filter_map(|observation| {
                let append_seq = fact_append_sequences
                    .get(observation.id.as_str())
                    .copied()?;
                identity_replay_event(observation, append_seq)
            })
            .map(|event| identity_replay_event_projection_item(&event))
            .collect::<Result<Vec<_>, _>>()?;
        let mut built = ProjectionSnapshot::build_with_state(
            observations,
            persisted_supplementals,
            freshness_thresholds,
            channels,
            stats,
            built_at,
        )?;
        normalize_person_fact_ids(&mut built.snapshot.person_page, &fact_append_sequences)?;
        let person_components = person_component_aggregates(
            &built.snapshot.identity,
            &built.snapshot.person_page,
            &built.person_consents,
        )?;
        let person_slide_count =
            u64::try_from(built.snapshot.person_page.slides.len()).map_err(|_| {
                SelfHostError::Ingestion("person slide count does not fit u64".to_owned())
            })?;
        let (projection_item_commit, person_message_count, reply_slo_count) =
            detach_projection_items(
                &mut built.snapshot,
                &built.compact_state,
                &person_components,
                identity_event_items,
            )?;
        let materialized = Self {
            format_version: NON_CORPUS_MATERIALIZATION_VERSION,
            last_append_seq: stats.max_append_seq,
            observation_count: stats.count,
            canonical_observation_fingerprint: built.canonical_observation_fingerprint,
            supplemental_fingerprint: built.supplemental_fingerprint,
            compact_state: built.compact_state,
            person_consents: built.person_consents,
            person_components,
            identity_event_count,
            person_slide_count,
            person_message_count,
            reply_slo_count,
            communication_projection: built.communication_projection,
            snapshot: built.snapshot,
            pending_item_commit: Some(PendingProjectionItemCommit {
                commit: projection_item_commit,
            }),
        };
        materialized.validate()?;
        Ok(materialized)
    }
}

fn rematerialize_communication_privacy_keys(
    projection: &mut CommunicationProjectionState,
    privacy_keys: &BTreeSet<String>,
    lookup: &dyn ComponentProjectionLookup,
    join_index: &ReplySloJoinIndex,
    delta: &mut ReplySloProjection,
) -> Result<(), SelfHostError> {
    let mut rematerialized_ids = BTreeSet::new();
    for privacy_key in privacy_keys {
        let mut cursor = 0_u64;
        loop {
            let page = lookup.observations_for_privacy_key_page(
                privacy_key,
                cursor,
                COMMUNICATION_RECONSENT_PAGE_SIZE,
            )?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|stored| stored.append_seq).ok_or_else(|| {
                SelfHostError::Ingestion("privacy reverse-index page unexpectedly empty".to_owned())
            })?;
            let observations = page
                .iter()
                .filter(|stored| {
                    rematerialized_ids.insert(stored.observation.id.as_str().to_owned())
                })
                .map(|stored| stored.observation.clone())
                .collect::<Vec<_>>();
            merge_reply_slo_delta(
                delta,
                projection.rematerialize_observations(&observations, join_index),
            );
        }
    }
    Ok(())
}

fn merge_reply_slo_delta(target: &mut ReplySloProjection, next: ReplySloProjection) {
    target.rows.extend(next.rows);
    target.rows.sort_by(|left, right| {
        left.due_at.cmp(&right.due_at).then_with(|| {
            left.incoming_observation_id
                .as_str()
                .cmp(right.incoming_observation_id.as_str())
        })
    });
    target.overdue = target
        .rows
        .iter()
        .filter(|row| {
            matches!(
                row.status,
                ReplySloStatus::Overdue | ReplySloStatus::SentLate
            )
        })
        .cloned()
        .collect();
}

fn apply_compact_incremental_delta(
    core: &mut AppCore,
    appended_observations: &[Observation],
    stats: ObservationStats,
    built_at: DateTime<Utc>,
    lookup: &dyn ComponentProjectionLookup,
) -> Result<ProjectionItemCommit, SelfHostError> {
    let fact_observations = appended_observations
        .iter()
        .filter(|observation| {
            observation.schema.as_str() == "schema:slack-message"
                || identity_replay_event(observation, 1).is_some()
        })
        .cloned()
        .collect::<Vec<_>>();
    let appended_fact_sequences =
        stored_observation_append_sequences(lookup, &fact_observations, stats.max_append_seq)?;
    apply_compact_incremental_delta_with_sequences(
        core,
        appended_observations,
        stats,
        built_at,
        &appended_fact_sequences,
        lookup,
    )
}

fn apply_compact_incremental_delta_with_sequences(
    core: &mut AppCore,
    appended_observations: &[Observation],
    stats: ObservationStats,
    built_at: DateTime<Utc>,
    appended_fact_sequences: &BTreeMap<String, u64>,
    lookup: &dyn ComponentProjectionLookup,
) -> Result<ProjectionItemCommit, SelfHostError> {
    if appended_observations.is_empty() {
        return Err(SelfHostError::Ingestion(
            "compact materialization delta must contain an appended observation".to_owned(),
        ));
    }
    let appended_count = u64::try_from(appended_observations.len()).map_err(|_| {
        SelfHostError::Ingestion("appended observation count does not fit u64".to_owned())
    })?;
    let expected_count = core
        .observation_stats
        .count
        .checked_add(appended_count)
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "canonical observation count overflow during incremental materialization"
                    .to_owned(),
            )
        })?;
    if stats.count != expected_count {
        return Err(SelfHostError::Ingestion(format!(
            "incremental materialization expected {expected_count} canonical observations, but storage reports {}",
            stats.count
        )));
    }
    if stats.max_append_seq <= core.observation_stats.max_append_seq {
        return Err(SelfHostError::Ingestion(format!(
            "incremental materialization append sequence did not advance beyond {}",
            core.observation_stats.max_append_seq
        )));
    }

    if core.supplemental_projection_cache.count() != core.supplemental_count {
        return Err(SelfHostError::Ingestion(
            "resident supplemental reducer count diverged from supplemental store".to_owned(),
        ));
    }
    let current_supplemental_fingerprint = core.resident_supplemental_fingerprint.clone();
    if current_supplemental_fingerprint != core.supplemental_fingerprint {
        return Err(SelfHostError::Ingestion(
            "resident supplemental state does not match materialized supplemental fingerprint"
                .to_owned(),
        ));
    }
    let canonical_observation_fingerprint = append_canonical_observation_fingerprint(
        &core.canonical_observation_fingerprint,
        appended_observations,
    )?;
    let mut consent_privacy_keys = BTreeSet::new();
    for observation in appended_observations {
        let Some(decision) = consent_decision_from_observation(observation) else {
            continue;
        };
        consent_privacy_keys.extend(consent_decision_keys(&decision));
    }
    let compact_apply = core
        .compact_state
        .apply_observation_page(appended_observations)?;
    let mut affected_person_ids = compact_apply.affected_person_ids;
    let mut next_person_ids = BTreeSet::new();
    for node_id in &compact_apply.touched_nodes {
        let person_id = core.compact_state.person_id_for_node(*node_id)?;
        affected_person_ids.insert(person_id.clone());
        next_person_ids.insert(person_id);
    }

    let mut previous_components = BTreeMap::new();
    for person_id in &affected_person_ids {
        if let Some(component) = core.person_components.remove(person_id) {
            previous_components.insert(person_id.clone(), component);
        }
        core.person_consents.remove(person_id);
    }
    let previous_component_rows = previous_components
        .iter()
        .map(|(person_id, component)| {
            Ok((
                person_id.clone(),
                person_component_projection_item(component)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, SelfHostError>>()?;
    let previous_visible_rows = previous_components
        .values()
        .map(component_visible_row_count)
        .sum::<usize>();

    let mut previous_by_next = BTreeMap::<String, Vec<PersonComponentAggregate>>::new();
    for (previous_id, component) in previous_components {
        let seed = identity_component_seed(&previous_id)?;
        let next_id = core.compact_state.person_id_for_node(seed)?;
        next_person_ids.insert(next_id.clone());
        previous_by_next.entry(next_id).or_default().push(component);
    }

    let mut next_components = BTreeMap::new();
    let mut newly_opted_out = BTreeSet::new();
    for person_id in &next_person_ids {
        let person = core
            .compact_state
            .identity
            .resolved_person(person_id, "1.0.0")
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "affected identity component {person_id} has no aggregate"
                ))
            })?;
        let consent = core.compact_state.person_consent(&person);
        let previous = previous_by_next.remove(person_id).unwrap_or_default();
        if consent == ConsentStatus::OptedOut
            && previous
                .iter()
                .any(|component| component.consent != ConsentStatus::OptedOut)
        {
            newly_opted_out.insert(person_id.clone());
        }
        let component = merge_person_component(person, consent, previous)?;
        core.person_consents.insert(person_id.clone(), consent);
        next_components.insert(person_id.clone(), component);
    }
    if !previous_by_next.is_empty() {
        return Err(SelfHostError::Ingestion(
            "affected component aggregate was not assigned to a current identity component"
                .to_owned(),
        ));
    }

    let mut message_inserts = Vec::new();
    for observation in appended_observations
        .iter()
        .filter(|observation| observation.schema.as_str() == "schema:slack-message")
    {
        let node_id = slack_identity_node_for_observation(&core.compact_state, observation)?;
        let person_id = core.compact_state.person_id_for_node(node_id)?;
        let component = next_components.get_mut(&person_id).ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "Slack observation {} resolved to unaffected component {person_id}",
                observation.id
            ))
        })?;
        if component.consent == ConsentStatus::OptedOut {
            continue;
        }
        let append_seq = appended_fact_sequences
            .get(observation.id.as_str())
            .copied()
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "Slack message {} has no append sequence",
                    observation.id
                ))
            })?;
        let mut message = person_message_from_slack(observation, &person_id);
        message.id = materialized_message_id(append_seq, &observation.id);
        add_message_to_component(component, &message)?;
        message_inserts.push(person_message_projection_item(
            &message,
            &core.compact_state,
        )?);
    }

    let identity_event_inserts = appended_observations
        .iter()
        .filter_map(|observation| {
            let append_seq = appended_fact_sequences
                .get(observation.id.as_str())
                .copied()?;
            identity_replay_event(observation, append_seq)
        })
        .map(|event| identity_replay_event_projection_item(&event))
        .collect::<Result<Vec<_>, _>>()?;
    let mut reply_slo_delta = core.communication_projection.fold_observations(
        appended_observations,
        &core.supplemental_projection_cache.reply_slo,
    );
    rematerialize_communication_privacy_keys(
        &mut core.communication_projection,
        &consent_privacy_keys,
        lookup,
        &core.supplemental_projection_cache.reply_slo,
        &mut reply_slo_delta,
    )?;
    let reply_slo_inserts = reply_slo_delta
        .rows
        .iter()
        .map(reply_slo_projection_item)
        .collect::<Result<Vec<_>, _>>()?;

    let mut component_inserts = Vec::new();
    let mut component_updates = Vec::new();
    let mut component_deletes = previous_component_rows
        .keys()
        .filter(|person_id| !next_components.contains_key(*person_id))
        .map(|person_id| format!("person-component:{person_id}"))
        .collect::<Vec<_>>();
    for (person_id, component) in &next_components {
        let desired = person_component_projection_item(component)?;
        match previous_component_rows.get(person_id) {
            Some(previous) if previous == &desired => {}
            Some(_) => component_updates.push(desired),
            None => component_inserts.push(desired),
        }
    }

    let mut fact_deletes = Vec::new();
    let mut deleted_message_count = 0_u64;
    let mut deleted_slide_count = 0_u64;
    for component in next_components.values() {
        if !newly_opted_out.contains(component.person.person_id.as_str()) {
            continue;
        }
        let members = core
            .compact_state
            .identity
            .component_members_for_person(component.person.person_id.as_str())
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "opted-out component {} has no identity members",
                    component.person.person_id
                ))
            })?;
        for node_id in members {
            for item in lookup.person_message_items(&identity_node_owner(*node_id))? {
                if item.item_key.starts_with("pm:") {
                    deleted_message_count =
                        deleted_message_count.checked_add(1).ok_or_else(|| {
                            SelfHostError::Ingestion(
                                "person message delete count overflow".to_owned(),
                            )
                        })?;
                    fact_deletes.push(item.item_key);
                } else if item.item_key.starts_with("ps:") {
                    deleted_slide_count = deleted_slide_count.checked_add(1).ok_or_else(|| {
                        SelfHostError::Ingestion("person slide delete count overflow".to_owned())
                    })?;
                    fact_deletes.push(item.item_key);
                }
            }
        }
    }
    component_deletes.append(&mut fact_deletes);

    let inserted_message_count = u64::try_from(message_inserts.len()).map_err(|_| {
        SelfHostError::Ingestion("person message insert count does not fit u64".to_owned())
    })?;
    core.person_message_count = core
        .person_message_count
        .checked_add(inserted_message_count)
        .and_then(|count| count.checked_sub(deleted_message_count))
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "person message count overflow or underflow during incremental materialization"
                    .to_owned(),
            )
        })?;
    core.person_slide_count = core
        .person_slide_count
        .checked_sub(deleted_slide_count)
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "person slide count underflow during incremental materialization".to_owned(),
            )
        })?;
    core.identity_event_count = core
        .identity_event_count
        .checked_add(u64::try_from(identity_event_inserts.len()).map_err(|_| {
            SelfHostError::Ingestion("identity event delta count does not fit u64".to_owned())
        })?)
        .ok_or_else(|| SelfHostError::Ingestion("identity event count overflow".to_owned()))?;
    core.reply_slo_count = u64::try_from(core.communication_projection.len())
        .map_err(|_| SelfHostError::Ingestion("reply SLO count does not fit u64".to_owned()))?;

    let next_visible_rows = next_components
        .values()
        .map(component_visible_row_count)
        .sum::<usize>();
    for (person_id, component) in next_components {
        core.person_components.insert(person_id, component);
    }
    let output_count = core
        .snapshot
        .lineage
        .output_count
        .checked_sub(previous_visible_rows)
        .and_then(|count| count.checked_add(next_visible_rows))
        .and_then(|count| count.checked_add(usize::try_from(inserted_message_count).ok()?))
        .and_then(|count| count.checked_sub(usize::try_from(deleted_message_count).ok()?))
        .and_then(|count| count.checked_sub(usize::try_from(deleted_slide_count).ok()?))
        .ok_or_else(|| {
            SelfHostError::Ingestion("person-page output count overflow or underflow".to_owned())
        })?;

    core.snapshot.freshness = freshness_projection_after_delta(
        &core.snapshot.freshness,
        &core.freshness_thresholds,
        appended_observations,
        built_at,
    )?;
    if core.claim_queue_dirty {
        core.snapshot.claim_queue = core.supplemental_projection_cache.claim_queue();
    }
    (core.snapshot.resume_snapshot, core.snapshot.plan_state) = core
        .supplemental_projection_cache
        .cognition(&core.snapshot.claim_queue, built_at);
    core.snapshot.card_queue = core
        .supplemental_projection_cache
        .card_queue
        .projection(built_at);
    core.snapshot.identity = IdentityResolutionOutput::default();
    core.snapshot.person_page = PersonPageOutput::default();
    core.snapshot.reply_slo = ReplySloProjection::default();
    core.snapshot.built_at = built_at;
    core.snapshot.lineage = build_person_page_lineage(
        &canonical_observation_fingerprint,
        stats,
        &current_supplemental_fingerprint,
        core.supplemental_count,
        output_count,
        built_at,
    );
    core.observation_stats = stats;
    core.canonical_observation_fingerprint = canonical_observation_fingerprint;

    let commit = ProjectionItemCommit::Delta {
        inserts: message_inserts
            .into_iter()
            .chain(reply_slo_inserts)
            .chain(identity_event_inserts)
            .chain(component_inserts)
            .collect(),
        updates: component_updates,
        deletes: component_deletes,
    };
    commit.validate()?;
    Ok(commit)
}

fn identity_component_seed(person_id: &str) -> Result<IdentityNodeId, SelfHostError> {
    person_id
        .strip_prefix("person:component-")
        .ok_or_else(|| {
            SelfHostError::Ingestion(format!("invalid person component ID {person_id}"))
        })?
        .parse::<IdentityNodeId>()
        .map_err(|error| {
            SelfHostError::Ingestion(format!("invalid person component ID {person_id}: {error}"))
        })
}

fn component_visible_row_count(component: &PersonComponentAggregate) -> usize {
    usize::from(component.profile.is_some()) + usize::from(component.activity.is_some())
}

fn component_activity_fact_weight(activity: &PersonActivity) -> Result<u64, SelfHostError> {
    u64::try_from(activity.total_slides_related)
        .ok()
        .and_then(|slides| {
            u64::try_from(activity.total_messages)
                .ok()
                .and_then(|messages| slides.checked_add(messages))
        })
        .ok_or_else(|| SelfHostError::Ingestion("component fact weight overflow".to_owned()))
}

fn merge_person_component(
    person: ResolvedPerson,
    consent: ConsentStatus,
    previous: Vec<PersonComponentAggregate>,
) -> Result<PersonComponentAggregate, SelfHostError> {
    let mut activity = PersonActivity {
        person_id: person.person_id.clone(),
        total_slides_related: 0,
        total_messages: 0,
        first_activity: None,
        last_activity: None,
        active_channels: Vec::new(),
    };
    let mut fact_weight = 0_u64;
    let mut active_channels = BTreeSet::new();
    let mut slide_blob_refs = BTreeSet::new();
    let mut selected_frontend = None::<(FrontendProfileRank, FrontendProfile)>;
    for mut component in previous {
        fact_weight = fact_weight
            .checked_add(component.fact_weight)
            .ok_or_else(|| SelfHostError::Ingestion("component fact weight overflow".to_owned()))?;
        if active_channels.len() < component.active_channels.len() {
            std::mem::swap(&mut active_channels, &mut component.active_channels);
        }
        active_channels.extend(component.active_channels);
        if slide_blob_refs.len() < component.slide_blob_refs.len() {
            std::mem::swap(&mut slide_blob_refs, &mut component.slide_blob_refs);
        }
        slide_blob_refs.extend(component.slide_blob_refs);
        if let Some(previous_activity) = component.activity {
            merge_person_activity(&mut activity, previous_activity)?;
        }
        if let (Some(rank), Some(frontend)) =
            (component.frontend_profile_rank, component.frontend_profile)
        {
            let replace = selected_frontend.as_ref().is_none_or(|(current, _)| {
                (
                    current.richness_score,
                    current.created_at,
                    current.stable_id.as_str(),
                ) < (
                    rank.richness_score,
                    rank.created_at,
                    rank.stable_id.as_str(),
                )
            });
            if replace {
                selected_frontend = Some((rank, frontend));
            }
        }
    }
    activity.person_id = person.person_id.clone();
    activity.active_channels = active_channels.iter().cloned().collect();
    let frontend_profile_rank = selected_frontend.as_ref().map(|(rank, _)| rank.clone());
    let frontend_profile = selected_frontend.map(|(_, profile)| profile);
    let profile_updated_at = activity
        .last_activity
        .into_iter()
        .chain(activity.first_activity)
        .chain(frontend_profile_rank.as_ref().map(|rank| rank.created_at))
        .max()
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let profile = PersonProfile {
        person_id: person.person_id.clone(),
        display_name: person.canonical_name.clone(),
        self_intro_text: frontend_profile
            .as_ref()
            .and_then(|profile| profile.profile.bio_text.clone()),
        self_intro_slide_id: frontend_profile
            .as_ref()
            .map(|profile| profile.source_document_id.clone()),
        self_intro_thumbnail: frontend_profile.as_ref().and_then(|profile| {
            profile
                .thumbnail_url
                .clone()
                .or_else(|| profile.thumbnail_ref.clone())
        }),
        identities: person_identity_info(&person),
        source_count: person.sources.len(),
        last_activity: activity.last_activity,
        profile_updated_at,
        frontend_profile: frontend_profile.clone(),
        frontend_profile_created_at: frontend_profile_rank.as_ref().map(|rank| rank.created_at),
    };
    let visible = consent != ConsentStatus::OptedOut;
    Ok(PersonComponentAggregate {
        person,
        consent,
        fact_weight: if visible { fact_weight } else { 0 },
        active_channels: if visible {
            active_channels
        } else {
            BTreeSet::new()
        },
        slide_blob_refs: if visible {
            slide_blob_refs
        } else {
            BTreeSet::new()
        },
        frontend_profile_rank: visible.then_some(frontend_profile_rank).flatten(),
        frontend_profile: visible.then_some(frontend_profile.clone()).flatten(),
        profile: visible.then_some(profile),
        activity: visible.then_some(activity),
    })
}

fn merge_person_activity(
    target: &mut PersonActivity,
    source: PersonActivity,
) -> Result<(), SelfHostError> {
    target.total_slides_related = target
        .total_slides_related
        .checked_add(source.total_slides_related)
        .ok_or_else(|| SelfHostError::Ingestion("person slide aggregate overflow".to_owned()))?;
    target.total_messages = target
        .total_messages
        .checked_add(source.total_messages)
        .ok_or_else(|| SelfHostError::Ingestion("person message aggregate overflow".to_owned()))?;
    target.first_activity = target
        .first_activity
        .into_iter()
        .chain(source.first_activity)
        .min();
    target.last_activity = target
        .last_activity
        .into_iter()
        .chain(source.last_activity)
        .max();
    Ok(())
}

fn add_message_to_component(
    component: &mut PersonComponentAggregate,
    message: &PersonMessage,
) -> Result<(), SelfHostError> {
    component.fact_weight = component
        .fact_weight
        .checked_add(1)
        .ok_or_else(|| SelfHostError::Ingestion("component fact weight overflow".to_owned()))?;
    component.active_channels.insert(message.channel.clone());
    let activity = component.activity.as_mut().ok_or_else(|| {
        SelfHostError::Ingestion(format!(
            "visible message {} has no component activity",
            message.id
        ))
    })?;
    let profile = component.profile.as_mut().ok_or_else(|| {
        SelfHostError::Ingestion(format!(
            "visible message {} has no component profile",
            message.id
        ))
    })?;
    add_message_to_profile_activity(profile, activity, message)?;
    activity.active_channels = component.active_channels.iter().cloned().collect();
    Ok(())
}

fn add_message_to_profile_activity(
    profile: &mut PersonProfile,
    activity: &mut PersonActivity,
    message: &PersonMessage,
) -> Result<(), SelfHostError> {
    activity.total_messages = activity
        .total_messages
        .checked_add(1)
        .ok_or_else(|| SelfHostError::Ingestion("person message aggregate overflow".to_owned()))?;
    activity.first_activity = Some(
        activity
            .first_activity
            .map(|current| current.min(message.ts))
            .unwrap_or(message.ts),
    );
    activity.last_activity = Some(
        activity
            .last_activity
            .map(|current| current.max(message.ts))
            .unwrap_or(message.ts),
    );
    if !activity.active_channels.contains(&message.channel) {
        activity.active_channels.push(message.channel.clone());
        activity.active_channels.sort();
    }
    profile.last_activity = activity.last_activity;
    profile.profile_updated_at = profile.profile_updated_at.max(message.ts);
    Ok(())
}

fn slack_identity_node_for_observation(
    state: &CompactProjectionState,
    observation: &Observation,
) -> Result<IdentityNodeId, SelfHostError> {
    let user_id = observation
        .payload
        .get("user_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "Slack message {} has no identity node user_id",
                observation.id
            ))
        })?;
    state
        .nodes_by_observation_id
        .get(observation.id.as_str())
        .into_iter()
        .flatten()
        .copied()
        .find(|node_id| {
            state.identity.node(*node_id).is_some_and(|node| {
                node.candidate.identifiers.iter().any(|identifier| {
                    identifier.identifier_type == IdentifierType::UserId
                        && identifier.value == user_id
                })
            })
        })
        .ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "Slack message {} has no matching identity node",
                observation.id
            ))
        })
}

fn classify_non_corpus_delta_with_reason(
    observations: &[Observation],
) -> NonCorpusDeltaClassification {
    if observations.is_empty() {
        return NonCorpusDeltaClassification {
            kind: NonCorpusDeltaKind::NoOp,
        };
    }
    let mut saw_slack_message = false;
    let mut saw_communication = false;
    let mut saw_declared_schema = false;
    for observation in observations {
        let Some(_behavior) = projection_fold_behavior(observation.schema.as_str()) else {
            tracing::warn!(
                schema = %observation.schema,
                observation_id = %observation.id,
                "observation schema has no projection fold declaration; skipping"
            );
            continue;
        };
        saw_declared_schema = true;
        if observation.schema.as_str() == "schema:slack-message" {
            saw_slack_message = true;
        }
        if contributes_to_reply_slo(observation) {
            saw_communication = true;
        }
    }
    if !saw_declared_schema {
        return NonCorpusDeltaClassification {
            kind: NonCorpusDeltaKind::DeclaredSchemaSkip,
        };
    }
    if saw_communication {
        NonCorpusDeltaClassification {
            kind: NonCorpusDeltaKind::Communication,
        }
    } else if saw_slack_message {
        NonCorpusDeltaClassification {
            kind: NonCorpusDeltaKind::SlackMessage,
        }
    } else {
        NonCorpusDeltaClassification {
            kind: NonCorpusDeltaKind::FreshnessOnly,
        }
    }
}

fn contributes_to_reply_slo(observation: &Observation) -> bool {
    observation
        .meta
        .get("communication_channel_id")
        .and_then(serde_json::Value::as_str)
        .is_some()
        && observation
            .meta
            .get("communication_sender_id")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && observation
            .meta
            .get("communication_thread_ref")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && observation
            .meta
            .pointer("/communication/reply_due_at")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .is_some()
}

#[cfg(test)]
fn observation_append_sequences(
    observations: &[Observation],
    stats: ObservationStats,
) -> Result<BTreeMap<String, u64>, SelfHostError> {
    let count = u64::try_from(observations.len())
        .map_err(|_| SelfHostError::Ingestion("observation count does not fit u64".to_owned()))?;
    if count != stats.count {
        return Err(SelfHostError::Ingestion(format!(
            "fact provenance expected {} observations, received {count}",
            stats.count
        )));
    }
    if count == 0 {
        if stats.max_append_seq != 0 {
            return Err(SelfHostError::Ingestion(format!(
                "empty fact provenance has non-zero high-water {}",
                stats.max_append_seq
            )));
        }
        return Ok(BTreeMap::new());
    }
    let first_append_seq = stats
        .max_append_seq
        .checked_sub(count - 1)
        .filter(|first| *first > 0)
        .ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "canonical high-water {} cannot identify {count} fact sequences",
                stats.max_append_seq
            ))
        })?;
    observations
        .iter()
        .enumerate()
        .map(|(index, observation)| {
            let offset = u64::try_from(index).map_err(|_| {
                SelfHostError::Ingestion("fact sequence offset does not fit u64".to_owned())
            })?;
            let append_seq = first_append_seq.checked_add(offset).ok_or_else(|| {
                SelfHostError::Ingestion("fact append sequence overflow".to_owned())
            })?;
            Ok((observation.id.as_str().to_owned(), append_seq))
        })
        .collect()
}

fn stored_observation_append_sequences(
    lookup: &dyn ComponentProjectionLookup,
    observations: &[Observation],
    canonical_high_water: u64,
) -> Result<BTreeMap<String, u64>, SelfHostError> {
    let mut sequences = BTreeMap::new();
    let mut seen_append_sequences = BTreeSet::new();
    for observation in observations {
        let stored = lookup.stored_observation(&observation.id)?.ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "fact provenance references missing observation {}",
                observation.id
            ))
        })?;
        if stored.observation.id != observation.id {
            return Err(SelfHostError::Ingestion(format!(
                "fact provenance lookup returned {} for {}",
                stored.observation.id, observation.id
            )));
        }
        if stored.append_seq == 0 || stored.append_seq > canonical_high_water {
            return Err(SelfHostError::Ingestion(format!(
                "fact observation {} has append sequence {} outside high-water {canonical_high_water}",
                observation.id, stored.append_seq
            )));
        }
        if !seen_append_sequences.insert(stored.append_seq) {
            return Err(SelfHostError::Ingestion(format!(
                "fact provenance repeats append sequence {}",
                stored.append_seq
            )));
        }
        sequences.insert(observation.id.as_str().to_owned(), stored.append_seq);
    }
    Ok(sequences)
}

fn materialized_message_id(append_seq: u64, observation_id: &ObservationId) -> String {
    format!("pm:{append_seq:020}:{observation_id}")
}

fn materialized_slide_id(append_seq: u64, observation_id: &str, claim: &str) -> String {
    format!("ps:{append_seq:020}:{observation_id}:{claim}")
}

fn normalize_person_fact_ids(
    person_page: &mut PersonPageOutput,
    append_sequences: &BTreeMap<String, u64>,
) -> Result<(), SelfHostError> {
    let mut message_ids = BTreeSet::new();
    for message in &mut person_page.messages {
        let observation_id = message.source_observation_id.as_str();
        if observation_id.is_empty() || message.id != format!("pm:{observation_id}") {
            return Err(SelfHostError::Ingestion(format!(
                "person message {} has invalid source provenance",
                message.id
            )));
        }
        let append_seq = append_sequences
            .get(observation_id)
            .copied()
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "person message {} references observation outside fact provenance",
                    message.id
                ))
            })?;
        message.id = materialized_message_id(append_seq, &ObservationId::new(observation_id));
        if !message_ids.insert(message.id.clone()) {
            return Err(SelfHostError::Ingestion(format!(
                "person-page produced duplicate stable message id {}",
                message.id
            )));
        }
    }

    let mut slide_ids = BTreeSet::new();
    for slide in &mut person_page.slides {
        let observation_id = slide.source_observation_id.as_str();
        let prefix = format!("ps:{observation_id}:");
        let claim = slide.id.strip_prefix(&prefix).ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "person slide {} has invalid source provenance",
                slide.id
            ))
        })?;
        if observation_id.is_empty() || claim.is_empty() {
            return Err(SelfHostError::Ingestion(format!(
                "person slide {} has blank source provenance",
                slide.id
            )));
        }
        let append_seq = append_sequences
            .get(observation_id)
            .copied()
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "person slide {} references observation outside fact provenance",
                    slide.id
                ))
            })?;
        slide.id = materialized_slide_id(append_seq, observation_id, claim);
        if !slide_ids.insert(slide.id.clone()) {
            return Err(SelfHostError::Ingestion(format!(
                "person-page produced duplicate stable slide id {}",
                slide.id
            )));
        }
    }
    Ok(())
}

fn merge_non_slack_person_page(
    current: &mut PersonPageOutput,
    page: PersonPageOutput,
    person_consents: &BTreeMap<String, ConsentStatus>,
) -> Result<(), SelfHostError> {
    if !page.messages.is_empty() {
        return Err(SelfHostError::Ingestion(
            "non-Slack person-page page unexpectedly produced messages".to_owned(),
        ));
    }
    let activity_indexes = current
        .activities
        .iter()
        .enumerate()
        .map(|(index, activity)| (activity.person_id.as_str().to_owned(), index))
        .collect::<BTreeMap<_, _>>();

    for slide in page.slides {
        let person_id = slide.person_id.as_str();
        if person_consents.get(person_id) == Some(&ConsentStatus::OptedOut) {
            continue;
        }
        let activity_index = *activity_indexes.get(person_id).ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "paged person slide references unknown person {person_id}"
            ))
        })?;
        let activity = &mut current.activities[activity_index];
        activity.total_slides_related =
            activity
                .total_slides_related
                .checked_add(1)
                .ok_or_else(|| {
                    SelfHostError::Ingestion(format!("person slide count overflow for {person_id}"))
                })?;
        if let Some(last_modified) = slide.last_modified {
            activity.first_activity = Some(
                activity
                    .first_activity
                    .map(|current| current.min(last_modified))
                    .unwrap_or(last_modified),
            );
            activity.last_activity = Some(
                activity
                    .last_activity
                    .map(|current| current.max(last_modified))
                    .unwrap_or(last_modified),
            );
        }
        current.slides.push(slide);
    }

    for profile in &mut current.profiles {
        let activity_index = *activity_indexes
            .get(profile.person_id.as_str())
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "paged person profile has no activity for {}",
                    profile.person_id
                ))
            })?;
        let activity = &current.activities[activity_index];
        profile.last_activity = activity.last_activity;
        if let Some(last_activity) = activity.last_activity {
            profile.profile_updated_at = profile.profile_updated_at.max(last_activity);
        } else if let Some(first_activity) = activity.first_activity {
            profile.profile_updated_at = profile.profile_updated_at.max(first_activity);
        }
    }
    Ok(())
}

fn person_message_activity_count(activities: &[PersonActivity]) -> Result<u64, SelfHostError> {
    let mut count = 0_u64;
    let mut person_ids = BTreeSet::new();
    for activity in activities {
        if !person_ids.insert(activity.person_id.as_str()) {
            return Err(SelfHostError::Ingestion(format!(
                "proj:person-page contains duplicate activity for {}",
                activity.person_id
            )));
        }
        let activity_count = u64::try_from(activity.total_messages).map_err(|_| {
            SelfHostError::Ingestion(format!(
                "activity message count does not fit u64 for {}",
                activity.person_id
            ))
        })?;
        count = count.checked_add(activity_count).ok_or_else(|| {
            SelfHostError::Ingestion(
                "person message activity count overflow in materialized manifest".to_owned(),
            )
        })?;
    }
    Ok(count)
}

fn stable_fact_append_seq(id: &str, prefix: &str) -> Result<u64, SelfHostError> {
    let suffix = id.strip_prefix(prefix).ok_or_else(|| {
        SelfHostError::Ingestion(format!("stable fact id {id} does not start with {prefix}"))
    })?;
    let (append_seq, provenance) = suffix.split_once(':').ok_or_else(|| {
        SelfHostError::Ingestion(format!("stable fact id {id} has no provenance"))
    })?;
    let parsed = append_seq.parse::<u64>().map_err(|_| {
        SelfHostError::Ingestion(format!(
            "stable fact id {id} has an invalid append sequence"
        ))
    })?;
    if parsed == 0 || append_seq != format!("{parsed:020}") || provenance.is_empty() {
        return Err(SelfHostError::Ingestion(format!(
            "stable fact id {id} is not canonical"
        )));
    }
    Ok(parsed)
}

fn person_message_append_seq(message: &PersonMessage) -> Result<u64, SelfHostError> {
    stable_fact_append_seq(&message.id, "pm:")
}

fn person_slide_append_seq(slide: &PersonSlide) -> Result<u64, SelfHostError> {
    stable_fact_append_seq(&slide.id, "ps:")
}

fn person_message_sort_key(message: &PersonMessage) -> Result<String, SelfHostError> {
    Ok(format!(
        "{:020}:{}",
        person_message_append_seq(message)?,
        message.id
    ))
}

fn person_message_projection_item(
    message: &PersonMessage,
    compact_state: &CompactProjectionState,
) -> Result<ProjectionItem, SelfHostError> {
    let node_id =
        compact_state.fact_node(&message.source_observation_id, message.person_id.as_str())?;
    let stored = StoredPersonMessage {
        node_id,
        id: message.id.clone(),
        source_observation_id: message.source_observation_id.clone(),
        channel: message.channel.clone(),
        text: message.text.clone(),
        ts: message.ts,
        thread_ts: message.thread_ts.clone(),
        has_attachments: message.has_attachments,
    };
    let item = ProjectionItem {
        item_key: message.id.clone(),
        owner_key: identity_node_owner(node_id),
        sort_key: person_message_sort_key(message)?,
        value: serde_json::to_value(stored)?,
    };
    item.validate()?;
    Ok(item)
}

fn person_message_from_projection_item(
    item: &ProjectionItem,
    compact_state: &CompactProjectionState,
) -> Result<PersonMessage, SelfHostError> {
    item.validate()?;
    let stored: StoredPersonMessage = serde_json::from_value(item.value.clone())?;
    if serde_json::to_value(&stored)? != item.value {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} contains a non-canonical person message value",
            item.item_key
        )));
    }
    let person_id = compact_state.person_id_for_node(stored.node_id)?;
    let message = PersonMessage {
        id: stored.id,
        source_observation_id: stored.source_observation_id,
        person_id: EntityRef::new(person_id),
        channel: stored.channel,
        text: stored.text,
        ts: stored.ts,
        thread_ts: stored.thread_ts,
        has_attachments: stored.has_attachments,
    };
    if item.item_key != message.id
        || item.owner_key != identity_node_owner(stored.node_id)
        || item.sort_key != person_message_sort_key(&message)?
    {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} metadata does not match its person message value",
            item.item_key
        )));
    }
    Ok(message)
}

#[cfg(test)]
fn detach_person_messages(
    person_page: &mut PersonPageOutput,
    compact_state: &CompactProjectionState,
) -> Result<Vec<ProjectionItem>, SelfHostError> {
    let messages = std::mem::take(&mut person_page.messages);
    let mut items = messages
        .iter()
        .map(|message| person_message_projection_item(message, compact_state))
        .collect::<Result<Vec<_>, _>>()?;
    items.sort_by(|left, right| {
        left.owner_key
            .cmp(&right.owner_key)
            .then_with(|| left.sort_key.cmp(&right.sort_key))
            .then_with(|| left.item_key.cmp(&right.item_key))
    });
    Ok(items)
}

fn identity_node_owner(node_id: IdentityNodeId) -> String {
    format!("identity-node:{node_id:020}")
}

fn person_slide_projection_item(
    slide: &PersonSlide,
    compact_state: &CompactProjectionState,
) -> Result<ProjectionItem, SelfHostError> {
    let node_id =
        compact_state.fact_node(&slide.source_observation_id, slide.person_id.as_str())?;
    let stored = StoredPersonSlide {
        node_id,
        id: slide.id.clone(),
        source_observation_id: slide.source_observation_id.clone(),
        document_id: slide.document_id.clone(),
        title: slide.title.clone(),
        role: slide.role.clone(),
        last_seen_revision: slide.last_seen_revision.clone(),
        slide_count: slide.slide_count,
        thumbnail_ref: slide.thumbnail_ref.clone(),
        last_modified: slide.last_modified,
    };
    let item = ProjectionItem {
        item_key: slide.id.clone(),
        owner_key: identity_node_owner(node_id),
        sort_key: format!("{:020}:{}", person_slide_append_seq(slide)?, slide.id),
        value: serde_json::to_value(stored)?,
    };
    item.validate()?;
    Ok(item)
}

fn person_slide_from_projection_item(
    item: &ProjectionItem,
    compact_state: &CompactProjectionState,
) -> Result<PersonSlide, SelfHostError> {
    item.validate()?;
    let stored: StoredPersonSlide = serde_json::from_value(item.value.clone())?;
    if serde_json::to_value(&stored)? != item.value
        || item.item_key != stored.id
        || item.owner_key != identity_node_owner(stored.node_id)
    {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} is not a canonical person slide fact",
            item.item_key
        )));
    }
    let person_id = compact_state.person_id_for_node(stored.node_id)?;
    let slide = PersonSlide {
        id: stored.id,
        source_observation_id: stored.source_observation_id,
        person_id: EntityRef::new(person_id),
        document_id: stored.document_id,
        title: stored.title,
        role: stored.role,
        last_seen_revision: stored.last_seen_revision,
        slide_count: stored.slide_count,
        thumbnail_ref: stored.thumbnail_ref,
        last_modified: stored.last_modified,
    };
    if item.sort_key != format!("{:020}:{}", person_slide_append_seq(&slide)?, slide.id) {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} has a non-canonical slide sort key",
            item.item_key
        )));
    }
    Ok(slide)
}

fn identity_replay_event(
    observation: &Observation,
    append_seq: u64,
) -> Option<IdentityReplayEvent> {
    let candidates = IdentityProjector::extract_candidates(std::slice::from_ref(observation));
    let consent_decision = compact_consent_decision_from_observation(observation);
    (!candidates.is_empty() || consent_decision.is_some()).then(|| IdentityReplayEvent {
        append_seq,
        observation_id: observation.id.as_str().to_owned(),
        candidates,
        consent_decision,
    })
}

fn identity_replay_event_projection_item(
    event: &IdentityReplayEvent,
) -> Result<ProjectionItem, SelfHostError> {
    let item = ProjectionItem {
        item_key: format!(
            "identity-event:{:020}:{}",
            event.append_seq, event.observation_id
        ),
        owner_key: IDENTITY_EVENT_ITEM_OWNER.to_owned(),
        sort_key: format!("{:020}:{}", event.append_seq, event.observation_id),
        value: serde_json::to_value(event)?,
    };
    item.validate()?;
    Ok(item)
}

fn identity_replay_event_from_projection_item(
    item: &ProjectionItem,
) -> Result<IdentityReplayEvent, SelfHostError> {
    item.validate()?;
    let event: IdentityReplayEvent = serde_json::from_value(item.value.clone())?;
    let expected = identity_replay_event_projection_item(&event)?;
    if expected != *item {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} is not a canonical identity replay event",
            item.item_key
        )));
    }
    Ok(event)
}

fn person_component_projection_item(
    component: &PersonComponentAggregate,
) -> Result<ProjectionItem, SelfHostError> {
    validate_person_component(component)?;
    let person_id = component.person.person_id.as_str();
    let item = ProjectionItem {
        item_key: format!("person-component:{person_id}"),
        owner_key: PERSON_COMPONENT_ITEM_OWNER.to_owned(),
        sort_key: person_id.to_owned(),
        value: serde_json::to_value(component)?,
    };
    item.validate()?;
    Ok(item)
}

fn person_component_from_projection_item(
    item: &ProjectionItem,
) -> Result<PersonComponentAggregate, SelfHostError> {
    item.validate()?;
    let mut component: PersonComponentAggregate = serde_json::from_value(item.value.clone())?;
    if let Some(profile) = &mut component.profile {
        profile.frontend_profile = component.frontend_profile.clone();
        profile.frontend_profile_created_at = component
            .frontend_profile_rank
            .as_ref()
            .map(|rank| rank.created_at);
    }
    validate_person_component(&component)?;
    let expected = person_component_projection_item(&component)?;
    if expected != *item {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} is not a canonical person component aggregate",
            item.item_key
        )));
    }
    Ok(component)
}

fn validate_person_component(component: &PersonComponentAggregate) -> Result<(), SelfHostError> {
    let person_id = component.person.person_id.as_str();
    let visible = component.consent != ConsentStatus::OptedOut;
    if component.profile.is_some() != visible || component.activity.is_some() != visible {
        return Err(SelfHostError::Ingestion(format!(
            "person component {person_id} visibility disagrees with consent"
        )));
    }
    if !visible {
        if component.fact_weight != 0
            || !component.active_channels.is_empty()
            || !component.slide_blob_refs.is_empty()
            || component.frontend_profile_rank.is_some()
            || component.frontend_profile.is_some()
        {
            return Err(SelfHostError::Ingestion(format!(
                "opted-out person component {person_id} retains materialized aggregate data"
            )));
        }
        return Ok(());
    }
    let activity = component.activity.as_ref().ok_or_else(|| {
        SelfHostError::Ingestion(format!("person component {person_id} has no activity"))
    })?;
    let profile = component.profile.as_ref().ok_or_else(|| {
        SelfHostError::Ingestion(format!("person component {person_id} has no profile"))
    })?;
    if activity.person_id != component.person.person_id
        || profile.person_id != component.person.person_id
        || component.fact_weight != component_activity_fact_weight(activity)?
        || component.active_channels
            != activity
                .active_channels
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
    {
        return Err(SelfHostError::Ingestion(format!(
            "person component {person_id} aggregate counters are inconsistent"
        )));
    }
    match (
        &component.frontend_profile_rank,
        &component.frontend_profile,
        &profile.frontend_profile,
    ) {
        (None, None, None) => {
            if profile.frontend_profile_created_at.is_some() {
                return Err(SelfHostError::Ingestion(format!(
                    "person component {person_id} has an orphan frontend profile timestamp"
                )));
            }
        }
        (Some(rank), Some(stored), Some(profile_frontend)) => {
            if rank.richness_score != stored.profile.richness_score()
                || rank.stable_id != stored.source_document_id
                || profile.frontend_profile_created_at != Some(rank.created_at)
                || serde_json::to_value(stored)? != serde_json::to_value(profile_frontend)?
            {
                return Err(SelfHostError::Ingestion(format!(
                    "person component {person_id} frontend profile aggregate is inconsistent"
                )));
            }
        }
        _ => {
            return Err(SelfHostError::Ingestion(format!(
                "person component {person_id} frontend profile aggregate is incomplete"
            )));
        }
    }
    Ok(())
}

fn canonical_reply_slo_row(mut row: ReplyLatency) -> ReplyLatency {
    row.latency_seconds = row
        .sent_at
        .map(|sent_at| (sent_at - row.published).num_seconds());
    row.status = match row.sent_at {
        Some(sent_at) if sent_at <= row.due_at => ReplySloStatus::SentOnTime,
        Some(_) => ReplySloStatus::SentLate,
        None => ReplySloStatus::Pending,
    };
    row
}

fn reply_slo_sort_key(row: &ReplyLatency) -> Result<String, SelfHostError> {
    let timestamp_nanos = row.due_at.timestamp_nanos_opt().ok_or_else(|| {
        SelfHostError::Ingestion(format!(
            "reply SLO due_at is outside nanosecond range for {}",
            row.incoming_observation_id
        ))
    })?;
    let sortable_timestamp = u64::try_from(i128::from(timestamp_nanos) - i128::from(i64::MIN))
        .map_err(|_| {
            SelfHostError::Ingestion(format!(
                "reply SLO due_at sort key overflow for {}",
                row.incoming_observation_id
            ))
        })?;
    Ok(format!(
        "{sortable_timestamp:020}:{}",
        row.incoming_observation_id
    ))
}

fn reply_slo_projection_item(row: &ReplyLatency) -> Result<ProjectionItem, SelfHostError> {
    let row = canonical_reply_slo_row(row.clone());
    let item = ProjectionItem {
        item_key: format!("reply-slo:{}", row.incoming_observation_id),
        owner_key: REPLY_SLO_ITEM_OWNER.to_owned(),
        sort_key: reply_slo_sort_key(&row)?,
        value: serde_json::to_value(&row)?,
    };
    item.validate()?;
    Ok(item)
}

pub(super) fn reply_slo_from_projection_item(
    item: &ProjectionItem,
) -> Result<ReplyLatency, SelfHostError> {
    item.validate()?;
    let row: ReplyLatency = serde_json::from_value(item.value.clone())?;
    let canonical = canonical_reply_slo_row(row);
    if serde_json::to_value(&canonical)? != item.value
        || item.owner_key != REPLY_SLO_ITEM_OWNER
        || item.item_key != format!("reply-slo:{}", canonical.incoming_observation_id)
        || item.sort_key != reply_slo_sort_key(&canonical)?
    {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} is not a canonical reply SLO row",
            item.item_key
        )));
    }
    Ok(canonical)
}

fn queue_projection_items(
    claim_queue: &ClaimQueueProjection,
    card_queue: &CardQueueProjection,
) -> Result<Vec<ProjectionItem>, SelfHostError> {
    let mut items = Vec::with_capacity(claim_queue.groups.len() + card_queue.cards.len());
    for group in &claim_queue.groups {
        let item = ProjectionItem {
            item_key: format!("claim-group:{}", group.group_id),
            owner_key: CLAIM_QUEUE_ITEM_OWNER.to_owned(),
            sort_key: group.group_id.clone(),
            value: serde_json::to_value(group)?,
        };
        item.validate()?;
        items.push(item);
    }
    for card in &card_queue.cards {
        let timestamp_nanos = card.created_at.timestamp_nanos_opt().ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "reply card created_at is outside nanosecond range for {}",
                card.draft_id
            ))
        })?;
        let sortable_timestamp = u64::try_from(i128::from(timestamp_nanos) - i128::from(i64::MIN))
            .map_err(|_| {
                SelfHostError::Ingestion(format!(
                    "reply card created_at sort key overflow for {}",
                    card.draft_id
                ))
            })?;
        let item = ProjectionItem {
            item_key: format!("card:{}", card.draft_id),
            owner_key: CARD_QUEUE_ITEM_OWNER.to_owned(),
            sort_key: format!("{sortable_timestamp:020}:{}", card.draft_id),
            value: serde_json::to_value(card)?,
        };
        item.validate()?;
        items.push(item);
    }
    Ok(items)
}

#[cfg(test)]
fn detach_projection_items(
    snapshot: &mut ProjectionSnapshot,
    compact_state: &CompactProjectionState,
    person_components: &BTreeMap<String, PersonComponentAggregate>,
    identity_event_items: Vec<ProjectionItem>,
) -> Result<(ProjectionItemCommit, u64, u64), SelfHostError> {
    let mut items = detach_person_messages(&mut snapshot.person_page, compact_state)?;
    let person_message_count = u64::try_from(items.len()).map_err(|_| {
        SelfHostError::Ingestion(
            "person message item count does not fit u64 during full build".to_owned(),
        )
    })?;
    let slides = std::mem::take(&mut snapshot.person_page.slides);
    items.extend(
        slides
            .iter()
            .map(|slide| person_slide_projection_item(slide, compact_state))
            .collect::<Result<Vec<_>, _>>()?,
    );
    items.extend(identity_event_items);
    items.extend(
        person_components
            .values()
            .map(person_component_projection_item)
            .collect::<Result<Vec<_>, _>>()?,
    );

    let reply_rows = std::mem::take(&mut snapshot.reply_slo.rows);
    snapshot.reply_slo.overdue.clear();
    let reply_slo_count = u64::try_from(reply_rows.len()).map_err(|_| {
        SelfHostError::Ingestion(
            "reply SLO item count does not fit u64 during full build".to_owned(),
        )
    })?;
    items.extend(
        reply_rows
            .iter()
            .map(reply_slo_projection_item)
            .collect::<Result<Vec<_>, _>>()?,
    );
    items.sort_by(|left, right| {
        left.owner_key
            .cmp(&right.owner_key)
            .then_with(|| left.sort_key.cmp(&right.sort_key))
            .then_with(|| left.item_key.cmp(&right.item_key))
    });
    Ok((
        ProjectionItemCommit::Replace { items },
        person_message_count,
        reply_slo_count,
    ))
}

fn validate_pending_projection_item_commit(
    pending: &PendingProjectionItemCommit,
    compact_state: &CompactProjectionState,
    final_person_message_count: u64,
    final_reply_slo_count: u64,
    activities: &[PersonActivity],
) -> Result<(), SelfHostError> {
    pending.commit.validate()?;
    let ProjectionItemCommit::Replace { items } = &pending.commit else {
        return Err(SelfHostError::Ingestion(
            "full materialization must publish a replace item commit".to_owned(),
        ));
    };
    let mut messages_by_person = BTreeMap::<String, u64>::new();
    let mut person_message_count = 0_u64;
    let mut reply_slo_count = 0_u64;
    for item in items {
        if item.owner_key == REPLY_SLO_ITEM_OWNER {
            reply_slo_from_projection_item(item)?;
            reply_slo_count = reply_slo_count.checked_add(1).ok_or_else(|| {
                SelfHostError::Ingestion("reply SLO item count overflow".to_owned())
            })?;
        } else if item.item_key.starts_with("pm:") {
            let message = person_message_from_projection_item(item, compact_state)?;
            person_message_count = person_message_count.checked_add(1).ok_or_else(|| {
                SelfHostError::Ingestion("person message item count overflow".to_owned())
            })?;
            let count = messages_by_person
                .entry(message.person_id.as_str().to_owned())
                .or_default();
            *count = count.checked_add(1).ok_or_else(|| {
                SelfHostError::Ingestion("person component message count overflow".to_owned())
            })?;
        } else if item.item_key.starts_with("ps:") {
            person_slide_from_projection_item(item, compact_state)?;
        } else if item.owner_key == IDENTITY_EVENT_ITEM_OWNER {
            identity_replay_event_from_projection_item(item)?;
        } else if item.owner_key == PERSON_COMPONENT_ITEM_OWNER {
            person_component_from_projection_item(item)?;
        } else {
            return Err(SelfHostError::Ingestion(format!(
                "projection item {} has an unknown keyed materialization kind",
                item.item_key
            )));
        }
    }
    if person_message_count != final_person_message_count
        || reply_slo_count != final_reply_slo_count
    {
        return Err(SelfHostError::Ingestion(
            "full materialization item counts disagree with manifest".to_owned(),
        ));
    }
    let activity_counts = activities
        .iter()
        .map(|activity| {
            Ok((
                activity.person_id.as_str().to_owned(),
                u64::try_from(activity.total_messages).map_err(|_| {
                    SelfHostError::Ingestion(format!(
                        "activity message count does not fit u64 for {}",
                        activity.person_id
                    ))
                })?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, SelfHostError>>()?;
    if messages_by_person
        .keys()
        .any(|person_id| !activity_counts.contains_key(person_id))
        || activity_counts.iter().any(|(person_id, expected)| {
            messages_by_person.get(person_id).copied().unwrap_or(0) != *expected
        })
    {
        return Err(SelfHostError::Ingestion(
            "full materialization message rows disagree with component activities".to_owned(),
        ));
    }
    Ok(())
}

fn person_message_from_slack(observation: &Observation, person_id: &str) -> PersonMessage {
    let text = observation
        .payload
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();
    let channel = observation
        .payload
        .get("channel_name")
        .or_else(|| observation.payload.get("channel"))
        .or_else(|| observation.payload.get("channel_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    PersonMessage {
        id: format!("pm:{}", observation.id),
        source_observation_id: observation.id.as_str().to_owned(),
        person_id: EntityRef::new(person_id),
        channel,
        text,
        ts: observation.published,
        thread_ts: observation
            .payload
            .get("thread_ts")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        has_attachments: !observation.attachments.is_empty(),
    }
}

fn person_identity_info(
    person: &lethe_engine::identity::types::ResolvedPerson,
) -> Vec<IdentityInfo> {
    person
        .identifiers
        .iter()
        .filter_map(|identifier| match identifier.identifier_type {
            IdentifierType::Email | IdentifierType::UserId => Some(IdentityInfo {
                system: identifier.source.clone(),
                external_id: identifier.value.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn freshness_projection_after_delta(
    current: &FreshnessProjection,
    thresholds: &[FreshnessThreshold],
    appended_observations: &[Observation],
    built_at: DateTime<Utc>,
) -> Result<FreshnessProjection, SelfHostError> {
    let mut current_by_source = BTreeMap::new();
    for source in &current.sources {
        if current_by_source
            .insert(source.source_id.clone(), source.clone())
            .is_some()
        {
            return Err(SelfHostError::Ingestion(format!(
                "materialized freshness contains duplicate source {}",
                source.source_id
            )));
        }
    }

    let mut latest = BTreeMap::new();
    for threshold in thresholds {
        let source = current_by_source
            .remove(&threshold.source_id)
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "materialized freshness is missing configured source {}",
                    threshold.source_id
                ))
            })?;
        if source.max_age_seconds != threshold.max_age_seconds {
            return Err(SelfHostError::Ingestion(format!(
                "materialized freshness threshold for {} is {}, expected {}",
                threshold.source_id, source.max_age_seconds, threshold.max_age_seconds
            )));
        }
        if source.latest_published.is_some() != source.latest_recorded_at.is_some() {
            return Err(SelfHostError::Ingestion(format!(
                "materialized freshness latest timestamps are incomplete for {}",
                threshold.source_id
            )));
        }
        let expected_last_observed = match (source.latest_published, source.latest_recorded_at) {
            (Some(published), Some(recorded_at)) => Some(published.max(recorded_at)),
            (None, None) => None,
            _ => unreachable!("presence equality was checked"),
        };
        if source.last_observed_at != expected_last_observed {
            return Err(SelfHostError::Ingestion(format!(
                "materialized freshness last_observed_at is inconsistent for {}",
                threshold.source_id
            )));
        }
        if latest
            .insert(
                threshold.source_id.clone(),
                (
                    threshold.max_age_seconds,
                    source.latest_published,
                    source.latest_recorded_at,
                ),
            )
            .is_some()
        {
            return Err(SelfHostError::Ingestion(format!(
                "freshness configuration contains duplicate source {}",
                threshold.source_id
            )));
        }
    }
    if let Some(unconfigured) = current_by_source.keys().next() {
        return Err(SelfHostError::Ingestion(format!(
            "materialized freshness contains unconfigured source {unconfigured}"
        )));
    }

    for observation in appended_observations {
        let source_id = freshness_source_id(observation)?;
        let Some((_, latest_published, latest_recorded_at)) = latest.get_mut(&source_id) else {
            continue;
        };
        *latest_published = Some(
            latest_published
                .map(|current| current.max(observation.published))
                .unwrap_or(observation.published),
        );
        *latest_recorded_at = Some(
            latest_recorded_at
                .map(|current| current.max(observation.recorded_at))
                .unwrap_or(observation.recorded_at),
        );
    }

    let sources = latest
        .into_iter()
        .map(
            |(source_id, (max_age_seconds, latest_published, latest_recorded_at))| {
                let last_observed_at = match (latest_published, latest_recorded_at) {
                    (Some(published), Some(recorded_at)) => Some(published.max(recorded_at)),
                    (None, None) => None,
                    _ => unreachable!("incremental timestamps are updated as a pair"),
                };
                let age_seconds =
                    last_observed_at.map(|last_observed| (built_at - last_observed).num_seconds());
                let status = match age_seconds {
                    Some(age) if age > max_age_seconds => FreshnessStatus::Missing,
                    Some(_) => FreshnessStatus::Fresh,
                    None => FreshnessStatus::Unobserved,
                };
                SourceFreshness {
                    source_id,
                    latest_published,
                    latest_recorded_at,
                    last_observed_at,
                    max_age_seconds,
                    age_seconds,
                    status,
                }
            },
        )
        .collect::<Vec<_>>();
    let missing = sources
        .iter()
        .filter(|source| {
            matches!(
                source.status,
                FreshnessStatus::Missing | FreshnessStatus::Unobserved
            )
        })
        .cloned()
        .collect();
    Ok(FreshnessProjection { sources, missing })
}

fn freshness_source_id(observation: &Observation) -> Result<String, SelfHostError> {
    observation
        .meta
        .get("communication_channel_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            observation
                .source_system
                .as_ref()
                .map(|source| source.as_str().to_owned())
        })
        .ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "observation {} has neither communication_channel_id nor source_system for freshness projection",
                observation.id
            ))
        })
}

fn person_page_output_count(
    person_page: &PersonPageOutput,
    person_slide_count: u64,
    person_message_count: u64,
) -> Result<usize, SelfHostError> {
    let person_slide_count = usize::try_from(person_slide_count).map_err(|_| {
        SelfHostError::Ingestion("person slide output count does not fit usize".to_owned())
    })?;
    let person_message_count = usize::try_from(person_message_count).map_err(|_| {
        SelfHostError::Ingestion("person message output count does not fit usize".to_owned())
    })?;
    person_page
        .profiles
        .len()
        .checked_add(person_slide_count)
        .and_then(|count| count.checked_add(person_message_count))
        .and_then(|count| count.checked_add(person_page.activities.len()))
        .ok_or_else(|| SelfHostError::Ingestion("person-page output count overflow".to_owned()))
}

#[cfg(test)]
fn canonical_observation_fingerprint(
    observations: &[Observation],
) -> Result<String, SelfHostError> {
    let mut accumulator = [0_u8; 32];
    for observation in observations {
        add_observation_to_fingerprint(&mut accumulator, observation)?;
    }
    Ok(hex::encode(accumulator))
}

fn append_canonical_observation_fingerprint(
    current: &str,
    observations: &[Observation],
) -> Result<String, SelfHostError> {
    let mut accumulator = decode_canonical_observation_fingerprint(current)?;
    for observation in observations {
        add_observation_to_fingerprint(&mut accumulator, observation)?;
    }
    Ok(hex::encode(accumulator))
}

fn decode_canonical_observation_fingerprint(value: &str) -> Result<[u8; 32], SelfHostError> {
    let decoded = hex::decode(value).map_err(|error| {
        SelfHostError::Ingestion(format!(
            "invalid canonical observation fingerprint encoding: {error}"
        ))
    })?;
    decoded.try_into().map_err(|decoded: Vec<u8>| {
        SelfHostError::Ingestion(format!(
            "canonical observation fingerprint has {} bytes, expected 32",
            decoded.len()
        ))
    })
}

fn add_observation_to_fingerprint(
    accumulator: &mut [u8; 32],
    observation: &Observation,
) -> Result<(), SelfHostError> {
    let encoded = serde_json::to_vec(observation)?;
    let encoded_len = u64::try_from(encoded.len()).map_err(|_| {
        SelfHostError::Ingestion("serialized observation length does not fit u64".to_owned())
    })?;
    let mut hasher = Sha256::new();
    hasher.update(CANONICAL_OBSERVATION_FINGERPRINT_DOMAIN);
    hasher.update(encoded_len.to_be_bytes());
    hasher.update(encoded);
    let digest: [u8; 32] = hasher.finalize().into();
    add_modulo_256(accumulator, &digest);
    Ok(())
}

fn add_modulo_256(accumulator: &mut [u8; 32], value: &[u8; 32]) {
    let mut carry = 0_u16;
    for index in (0..accumulator.len()).rev() {
        let sum = u16::from(accumulator[index]) + u16::from(value[index]) + carry;
        accumulator[index] = sum as u8;
        carry = sum >> 8;
    }
}

fn person_page_build_id(
    canonical_observation_fingerprint: &str,
    observation_count: u64,
    supplemental_fingerprint: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"proj:person-page@1.0.0\n");
    hasher.update(b"canonical-observation-accumulator:v1\n");
    hasher.update(observation_count.to_be_bytes());
    hasher.update(canonical_observation_fingerprint.as_bytes());
    hasher.update(b"\n");
    hasher.update(supplemental_fingerprint.as_bytes());
    format!("build-{}", hex::encode(hasher.finalize()))
}

fn supplemental_fingerprint(
    records: &[lethe_core::domain::SupplementalRecord],
) -> Result<String, SelfHostError> {
    let mut accumulator = [0_u8; 32];
    for record in records {
        add_modulo_256(&mut accumulator, &supplemental_record_digest(record)?);
    }
    Ok(hex::encode(accumulator))
}

fn supplemental_fingerprint_after_delta(
    current: &str,
    previous: Option<&SupplementalRecord>,
    next: &SupplementalRecord,
) -> Result<String, SelfHostError> {
    let mut accumulator = decode_supplemental_fingerprint(current)?;
    if let Some(previous) = previous {
        subtract_modulo_256(&mut accumulator, &supplemental_record_digest(previous)?);
    }
    add_modulo_256(&mut accumulator, &supplemental_record_digest(next)?);
    Ok(hex::encode(accumulator))
}

fn supplemental_record_digest(record: &SupplementalRecord) -> Result<[u8; 32], SelfHostError> {
    let encoded = serde_json::to_vec(record)?;
    let encoded_len = u64::try_from(encoded.len()).map_err(|_| {
        SelfHostError::Ingestion("serialized supplemental length does not fit u64".to_owned())
    })?;
    let mut hasher = Sha256::new();
    hasher.update(SUPPLEMENTAL_FINGERPRINT_DOMAIN);
    hasher.update(encoded_len.to_be_bytes());
    hasher.update(encoded);
    Ok(hasher.finalize().into())
}

fn decode_supplemental_fingerprint(value: &str) -> Result<[u8; 32], SelfHostError> {
    let bytes = hex::decode(value).map_err(|error| {
        SelfHostError::Ingestion(format!(
            "invalid supplemental fingerprint encoding: {error}"
        ))
    })?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        SelfHostError::Ingestion(format!(
            "supplemental fingerprint has {} bytes, expected 32",
            bytes.len()
        ))
    })
}

fn subtract_modulo_256(accumulator: &mut [u8; 32], value: &[u8; 32]) {
    let mut borrow = 0_i16;
    for index in (0..accumulator.len()).rev() {
        let difference = i16::from(accumulator[index]) - i16::from(value[index]) - borrow;
        if difference < 0 {
            accumulator[index] = (difference + 256) as u8;
            borrow = 1;
        } else {
            accumulator[index] = difference as u8;
            borrow = 0;
        }
    }
}

trait NonCorpusRebuildStorage {
    fn load_supplementals(&self) -> Result<Vec<SupplementalRecord>, SelfHostError>;
    fn observation_stats(&self) -> Result<ObservationStats, SelfHostError>;
    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError>;
    fn observations_for_privacy_key_page(
        &self,
        privacy_key: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError>;
    fn observation_by_id(
        &self,
        id: &ObservationId,
    ) -> Result<Option<StoredObservation>, SelfHostError>;
    fn commit_projection_items(
        &self,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        commit: &ProjectionItemCommit,
    ) -> Result<(), SelfHostError>;
    fn projection_item_count_by_owner(
        &self,
        projection: &ProjectionRef,
        owner_key: &str,
    ) -> Result<u64, SelfHostError>;
    fn publish_projection_items_from_staging(
        &self,
        target: &ProjectionRef,
        staging: &ProjectionRef,
        manifest: &serde_json::Value,
        expected_item_count: u64,
    ) -> Result<(), SelfHostError>;
    fn set_state(&self, key: &str, value: &str) -> Result<(), SelfHostError>;
}

impl<T: StoragePorts + ?Sized> NonCorpusRebuildStorage for T {
    fn load_supplementals(&self) -> Result<Vec<SupplementalRecord>, SelfHostError> {
        Ok(lethe_storage_api::SupplementalStore::load_supplementals(
            self,
        )?)
    }

    fn observation_stats(&self) -> Result<ObservationStats, SelfHostError> {
        Ok(lethe_storage_api::ObservationStore::observation_stats(
            self,
        )?)
    }

    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError> {
        Ok(lethe_storage_api::ObservationStore::observation_page(
            self,
            after_append_seq,
            limit,
        )?)
    }

    fn observations_for_privacy_key_page(
        &self,
        privacy_key: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError> {
        Ok(
            lethe_storage_api::ObservationStore::observations_for_privacy_key_page(
                self,
                privacy_key,
                after_append_seq,
                limit,
            )?,
        )
    }

    fn observation_by_id(
        &self,
        id: &ObservationId,
    ) -> Result<Option<StoredObservation>, SelfHostError> {
        Ok(lethe_storage_api::ObservationStore::observation_by_id(
            self, id,
        )?)
    }

    fn commit_projection_items(
        &self,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        commit: &ProjectionItemCommit,
    ) -> Result<(), SelfHostError> {
        Ok(
            lethe_storage_api::ProjectionMaterializer::commit_projection_items(
                self, projection, manifest, commit,
            )?,
        )
    }

    fn projection_item_count_by_owner(
        &self,
        projection: &ProjectionRef,
        owner_key: &str,
    ) -> Result<u64, SelfHostError> {
        Ok(
            lethe_storage_api::ProjectionMaterializer::projection_item_count_by_owner(
                self, projection, owner_key,
            )?,
        )
    }

    fn publish_projection_items_from_staging(
        &self,
        target: &ProjectionRef,
        staging: &ProjectionRef,
        manifest: &serde_json::Value,
        expected_item_count: u64,
    ) -> Result<(), SelfHostError> {
        Ok(
            lethe_storage_api::ProjectionMaterializer::publish_projection_items_from_staging(
                self,
                target,
                staging,
                manifest,
                expected_item_count,
            )?,
        )
    }

    fn set_state(&self, key: &str, value: &str) -> Result<(), SelfHostError> {
        Ok(lethe_storage_api::RuntimeStateStore::set_state(
            self, key, value,
        )?)
    }
}

fn for_each_observation_page(
    persistence: &dyn NonCorpusRebuildStorage,
    stats: ObservationStats,
    page_size: usize,
    mut visit: impl FnMut(&[StoredObservation], &[Observation]) -> Result<(), SelfHostError>,
) -> Result<(), SelfHostError> {
    if page_size == 0 {
        return Err(SelfHostError::Ingestion(
            "non-corpus rebuild page size must be greater than zero".to_owned(),
        ));
    }
    if stats.count == 0 {
        if stats.max_append_seq != 0 {
            return Err(SelfHostError::Ingestion(format!(
                "empty canonical lake has non-zero append high-water {}",
                stats.max_append_seq
            )));
        }
        return Ok(());
    }

    let mut after_append_seq = 0_u64;
    let mut seen = 0_u64;
    while seen < stats.count {
        let page = persistence.observation_page(after_append_seq, page_size)?;
        if page.is_empty() {
            return Err(SelfHostError::Ingestion(format!(
                "canonical observation paging ended after {seen} of {} rows",
                stats.count
            )));
        }
        if page.len() > page_size {
            return Err(SelfHostError::Ingestion(format!(
                "canonical observation page returned {} rows above configured limit {page_size}",
                page.len()
            )));
        }
        let bounded_page_len = page
            .iter()
            .take_while(|stored| stored.append_seq <= stats.max_append_seq)
            .count();
        let page = &page[..bounded_page_len];
        if page.is_empty() {
            return Err(SelfHostError::Ingestion(format!(
                "canonical observation paging crossed fixed high-water {} after {seen} of {} rows",
                stats.max_append_seq, stats.count
            )));
        }

        let mut observations = Vec::with_capacity(page.len());
        for stored in page {
            if stored.append_seq <= after_append_seq {
                return Err(SelfHostError::Ingestion(format!(
                    "canonical observation page is not strictly ordered after append sequence {after_append_seq}"
                )));
            }
            after_append_seq = stored.append_seq;
            seen = seen.checked_add(1).ok_or_else(|| {
                SelfHostError::Ingestion(
                    "canonical observation count overflow during paged rebuild".to_owned(),
                )
            })?;
            if seen > stats.count {
                return Err(SelfHostError::Ingestion(format!(
                    "canonical observation paging exceeded fixed count {}",
                    stats.count
                )));
            }
            observations.push(stored.observation.clone());
        }
        visit(page, &observations)?;
    }

    if seen != stats.count || after_append_seq != stats.max_append_seq {
        return Err(SelfHostError::Ingestion(format!(
            "canonical observation paging finished at count {seen}/append sequence {after_append_seq}, expected count {}/append sequence {}",
            stats.count, stats.max_append_seq
        )));
    }
    Ok(())
}

fn frontend_profiles_from_supplementals(
    persistence: &dyn NonCorpusRebuildStorage,
    identity: &IdentityResolutionOutput,
    person_consents: &BTreeMap<String, ConsentStatus>,
    supplementals: &[lethe_core::domain::SupplementalRecord],
    canonical_high_water: u64,
) -> Result<FrontendProfileSelections, SelfHostError> {
    let mut frontend_profiles = BTreeMap::new();
    for record in supplementals
        .iter()
        .filter(|record| record.kind == "slide-analysis")
    {
        let Some(observation_id) = record.derived_from.observations.first() else {
            continue;
        };
        let stored = persistence
            .observation_by_id(observation_id)?
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "slide-analysis supplemental {} references missing observation {observation_id}",
                    record.id
                ))
            })?;
        if stored.append_seq > canonical_high_water {
            return Err(SelfHostError::Ingestion(format!(
                "slide-analysis supplemental {} crossed canonical high-water {canonical_high_water}",
                record.id
            )));
        }
        for (person_id, (created_at, profile)) in PersonPageProjector::project_frontend_profiles(
            identity,
            std::slice::from_ref(&stored.observation),
            &[record],
        ) {
            if person_consents.get(&person_id) == Some(&ConsentStatus::OptedOut) {
                continue;
            }
            let richness = profile.profile.richness_score();
            let should_replace = frontend_profiles.get(&person_id).is_none_or(
                |(current_richness, current_created_at, current_profile): &(
                    usize,
                    DateTime<Utc>,
                    FrontendProfile,
                )| {
                    richness > *current_richness
                        || (richness == *current_richness
                            && (created_at > *current_created_at
                                || (created_at == *current_created_at
                                    && profile.source_document_id
                                        > current_profile.source_document_id)))
                },
            );
            if should_replace {
                frontend_profiles.insert(person_id, (richness, created_at, profile));
            }
        }
    }
    Ok(frontend_profiles)
}

fn install_frontend_profiles(
    person_page: &mut PersonPageOutput,
    mut frontend_profiles: FrontendProfileSelections,
) -> Result<(), SelfHostError> {
    let activity_by_person = person_page
        .activities
        .iter()
        .map(|activity| (activity.person_id.as_str().to_owned(), activity))
        .collect::<BTreeMap<_, _>>();
    for profile in &mut person_page.profiles {
        let activity = activity_by_person
            .get(profile.person_id.as_str())
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "person profile has no activity for {}",
                    profile.person_id
                ))
            })?;
        profile.self_intro_text = None;
        profile.self_intro_slide_id = None;
        profile.self_intro_thumbnail = None;
        profile.frontend_profile = None;
        profile.frontend_profile_created_at = None;
        profile.last_activity = activity.last_activity;
        profile.profile_updated_at = activity
            .last_activity
            .or(activity.first_activity)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        if let Some((_, created_at, frontend_profile)) =
            frontend_profiles.remove(profile.person_id.as_str())
        {
            profile.self_intro_text = frontend_profile.profile.bio_text.clone();
            profile.self_intro_slide_id = Some(frontend_profile.source_document_id.clone());
            profile.self_intro_thumbnail = frontend_profile
                .thumbnail_url
                .clone()
                .or_else(|| frontend_profile.thumbnail_ref.clone());
            profile.profile_updated_at = profile.profile_updated_at.max(created_at);
            profile.frontend_profile = Some(frontend_profile);
            profile.frontend_profile_created_at = Some(created_at);
        }
    }
    if !frontend_profiles.is_empty() {
        return Err(SelfHostError::Ingestion(
            "frontend profile resolved to a person absent from person-page output".to_owned(),
        ));
    }
    Ok(())
}

fn rebuild_materialized_snapshot_paged(
    persistence: &dyn NonCorpusRebuildStorage,
    supplementals: &[lethe_core::domain::SupplementalRecord],
    freshness_thresholds: &[FreshnessThreshold],
    channels: &[lethe_registry::registry::ChannelRecord],
    stats: ObservationStats,
    page_size: usize,
    built_at: DateTime<Utc>,
) -> Result<MaterializedProjectionSnapshot, SelfHostError> {
    let mut compact_state = CompactProjectionState::build(&[])?;
    let mut canonical_fingerprint = [0_u8; 32];
    let mut freshness =
        FreshnessProjector::new(freshness_thresholds.to_vec(), built_at).project_observations(&[]);
    let mut answer_log = Vec::new();
    let reply_slo_join_index = ReplySloJoinIndex::from_records(supplementals);
    let mut communication_projection = CommunicationProjectionState::default();
    let mut privacy_filter = PrivacyFilter::default();

    for_each_observation_page(persistence, stats, page_size, |_, observations| {
        privacy_filter.apply_observations(observations);
        compact_state.apply_observation_page(observations)?;
        communication_projection.fold_observations(observations, &reply_slo_join_index);
        let consent_privacy_keys = observations
            .iter()
            .filter_map(consent_decision_from_observation)
            .flat_map(|decision| consent_decision_keys(&decision))
            .collect::<BTreeSet<_>>();
        for privacy_key in consent_privacy_keys {
            let mut cursor = 0_u64;
            loop {
                let page = persistence.observations_for_privacy_key_page(
                    &privacy_key,
                    cursor,
                    page_size,
                )?;
                if page.is_empty() {
                    break;
                }
                let bounded_page = page
                    .iter()
                    .take_while(|stored| stored.append_seq <= stats.max_append_seq)
                    .collect::<Vec<_>>();
                if bounded_page.is_empty() {
                    break;
                }
                cursor = bounded_page
                    .last()
                    .map(|stored| stored.append_seq)
                    .ok_or_else(|| {
                        SelfHostError::Ingestion(
                            "privacy reverse-index page unexpectedly empty".to_owned(),
                        )
                    })?;
                let observations = bounded_page
                    .iter()
                    .map(|stored| stored.observation.clone())
                    .collect::<Vec<_>>();
                communication_projection
                    .rematerialize_observations(&observations, &reply_slo_join_index);
                if cursor == stats.max_append_seq
                    || page
                        .last()
                        .is_some_and(|stored| stored.append_seq > stats.max_append_seq)
                {
                    break;
                }
            }
        }
        for observation in observations {
            add_observation_to_fingerprint(&mut canonical_fingerprint, observation)?;
        }
        freshness = freshness_projection_after_delta(
            &freshness,
            freshness_thresholds,
            observations,
            built_at,
        )?;
        answer_log.extend(AnswerLogProjector.project_observations(observations));
        Ok(())
    })?;
    answer_log.sort_by(|left, right| {
        right
            .ts
            .cmp(&left.ts)
            .then_with(|| left.record_id.cmp(&right.record_id))
    });

    let identity = compact_state.resolve_identity();
    let person_consents = compact_state.person_consents(&identity);
    let mut person_page = PersonPageProjector::project(&identity, &[], &[]);
    person_page.profiles.retain(|profile| {
        person_consents.get(profile.person_id.as_str()) != Some(&ConsentStatus::OptedOut)
    });
    person_page.activities.retain(|activity| {
        person_consents.get(activity.person_id.as_str()) != Some(&ConsentStatus::OptedOut)
    });
    let profile_index = person_page
        .profiles
        .iter()
        .enumerate()
        .map(|(index, profile)| (profile.person_id.as_str().to_owned(), index))
        .collect::<BTreeMap<_, _>>();
    let activity_index = person_page
        .activities
        .iter()
        .enumerate()
        .map(|(index, activity)| (activity.person_id.as_str().to_owned(), index))
        .collect::<BTreeMap<_, _>>();
    if profile_index.keys().collect::<BTreeSet<_>>()
        != activity_index.keys().collect::<BTreeSet<_>>()
    {
        return Err(SelfHostError::Ingestion(
            "paged person profile/activity indexes differ".to_owned(),
        ));
    }

    let frontend_profiles = frontend_profiles_from_supplementals(
        persistence,
        &identity,
        &person_consents,
        supplementals,
        stats.max_append_seq,
    )?;

    let staging_projection = ProjectionRef::new(NON_CORPUS_REBUILD_STAGING_PROJECTION_ID);
    let staging_manifest = serde_json::json!({
        "format_version": 1,
        "state": "building",
        "target_projection": "proj:person-page",
        "canonical_count": stats.count,
        "canonical_high_water": stats.max_append_seq,
    });
    persistence.commit_projection_items(
        &staging_projection,
        &staging_manifest,
        &ProjectionItemCommit::Replace { items: Vec::new() },
    )?;

    let mut person_message_count = 0_u64;
    let mut reply_slo_count = 0_u64;
    let mut identity_event_count = 0_u64;
    for_each_observation_page(persistence, stats, page_size, |stored, observations| {
        let fact_append_sequences = stored
            .iter()
            .map(|stored| (stored.observation.id.as_str().to_owned(), stored.append_seq))
            .collect::<BTreeMap<_, _>>();
        let mut inserts = Vec::new();
        for stored_observation in stored {
            if let Some(event) = identity_replay_event(
                &stored_observation.observation,
                stored_observation.append_seq,
            ) {
                inserts.push(identity_replay_event_projection_item(&event)?);
                identity_event_count = identity_event_count.checked_add(1).ok_or_else(|| {
                    SelfHostError::Ingestion("identity event count overflow".to_owned())
                })?;
            }
        }
        for observation in observations.iter().filter(|observation| {
            observation.schema.as_str() == "schema:slack-message"
                && privacy_filter.visible(observation)
        }) {
            let node_id = slack_identity_node_for_observation(&compact_state, observation)?;
            let person_id = compact_state.person_id_for_node(node_id)?;
            if person_consents.get(&person_id) == Some(&ConsentStatus::OptedOut) {
                continue;
            }
            let append_seq = fact_append_sequences
                .get(observation.id.as_str())
                .copied()
                .ok_or_else(|| {
                    SelfHostError::Ingestion(format!(
                        "Slack message {} has no append sequence during paged rebuild",
                        observation.id
                    ))
                })?;
            let mut message = person_message_from_slack(observation, &person_id);
            message.id = materialized_message_id(append_seq, &observation.id);
            let profile = person_page.profiles.get_mut(profile_index[&person_id]);
            let activity = person_page.activities.get_mut(activity_index[&person_id]);
            match (profile, activity) {
                (Some(profile), Some(activity)) => {
                    add_message_to_profile_activity(profile, activity, &message)?;
                }
                _ => {
                    return Err(SelfHostError::Ingestion(format!(
                        "paged Slack component {person_id} has no profile/activity aggregate"
                    )));
                }
            }
            inserts.push(person_message_projection_item(&message, &compact_state)?);
        }

        let non_slack = observations
            .iter()
            .filter(|observation| {
                observation.schema.as_str() != "schema:slack-message"
                    && privacy_filter.visible(observation)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !non_slack.is_empty() {
            let mut page = PersonPageProjector::project(&identity, &non_slack, &[]);
            normalize_person_fact_ids(&mut page, &fact_append_sequences)?;
            merge_non_slack_person_page(&mut person_page, page, &person_consents)?;
        }

        let reply_slo = communication_projection.project_observations(observations, built_at);
        let page_reply_slo_count = u64::try_from(reply_slo.rows.len()).map_err(|_| {
            SelfHostError::Ingestion("paged reply SLO row count does not fit u64".to_owned())
        })?;
        inserts.extend(
            reply_slo
                .rows
                .iter()
                .map(reply_slo_projection_item)
                .collect::<Result<Vec<_>, _>>()?,
        );
        let page_person_message_count = u64::try_from(
            inserts
                .iter()
                .filter(|item| item.item_key.starts_with("pm:"))
                .count(),
        )
        .map_err(|_| {
            SelfHostError::Ingestion("paged person message row count does not fit u64".to_owned())
        })?;
        person_message_count = person_message_count
            .checked_add(page_person_message_count)
            .ok_or_else(|| {
                SelfHostError::Ingestion(
                    "person message count overflow during paged rebuild".to_owned(),
                )
            })?;
        reply_slo_count = reply_slo_count
            .checked_add(page_reply_slo_count)
            .ok_or_else(|| {
                SelfHostError::Ingestion("reply SLO count overflow during paged rebuild".to_owned())
            })?;

        if !inserts.is_empty() {
            persistence.commit_projection_items(
                &staging_projection,
                &staging_manifest,
                &ProjectionItemCommit::Delta {
                    inserts,
                    updates: Vec::new(),
                    deletes: Vec::new(),
                },
            )?;
        }
        Ok(())
    })?;

    let identity_order = identity
        .resolved_persons
        .iter()
        .enumerate()
        .map(|(index, person)| (person.person_id.as_str().to_owned(), index))
        .collect::<BTreeMap<_, _>>();
    let mut slide_sort_keys = BTreeMap::new();
    for slide in &person_page.slides {
        let identity_rank = *identity_order
            .get(slide.person_id.as_str())
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "paged slide {} references unknown person {}",
                    slide.id, slide.person_id
                ))
            })?;
        let sort_key = (identity_rank, person_slide_append_seq(slide)?);
        if slide_sort_keys.insert(slide.id.clone(), sort_key).is_some() {
            return Err(SelfHostError::Ingestion(format!(
                "paged person-page contains duplicate slide id {}",
                slide.id
            )));
        }
    }
    person_page
        .slides
        .sort_by_key(|slide| slide_sort_keys[&slide.id]);
    install_frontend_profiles(&mut person_page, frontend_profiles)?;
    let person_components = person_component_aggregates(&identity, &person_page, &person_consents)?;
    let person_slide_count = u64::try_from(person_page.slides.len())
        .map_err(|_| SelfHostError::Ingestion("person slide count does not fit u64".to_owned()))?;
    let mut keyed_state_items = person_page
        .slides
        .iter()
        .map(|slide| person_slide_projection_item(slide, &compact_state))
        .collect::<Result<Vec<_>, _>>()?;
    keyed_state_items.extend(
        person_components
            .values()
            .map(person_component_projection_item)
            .collect::<Result<Vec<_>, _>>()?,
    );
    let claim_queue = ClaimQueueProjector.project_records(supplementals);
    let card_queue = CardQueueProjector::new(built_at).project_records(supplementals);
    keyed_state_items.extend(queue_projection_items(&claim_queue, &card_queue)?);
    if !keyed_state_items.is_empty() {
        persistence.commit_projection_items(
            &staging_projection,
            &staging_manifest,
            &ProjectionItemCommit::Delta {
                inserts: keyed_state_items,
                updates: Vec::new(),
                deletes: Vec::new(),
            },
        )?;
    }
    person_page.slides.clear();

    let canonical_observation_fingerprint = hex::encode(canonical_fingerprint);
    let supplemental_fingerprint = supplemental_fingerprint(supplementals)?;
    let cognition_projector = CognitionStateProjector::new(built_at);
    let (resume_snapshot, plan_state) =
        cognition_projector.project_with_claim_queue(supplementals, &claim_queue);
    let lineage = build_person_page_lineage(
        &canonical_observation_fingerprint,
        stats,
        &supplemental_fingerprint,
        supplementals.len(),
        person_page_output_count(&person_page, person_slide_count, person_message_count)?,
        built_at,
    );
    let materialized = MaterializedProjectionSnapshot {
        format_version: NON_CORPUS_MATERIALIZATION_VERSION,
        last_append_seq: stats.max_append_seq,
        observation_count: stats.count,
        canonical_observation_fingerprint,
        supplemental_fingerprint,
        compact_state,
        person_consents,
        person_components,
        identity_event_count,
        person_slide_count,
        person_message_count,
        reply_slo_count,
        communication_projection,
        snapshot: ProjectionSnapshot {
            identity,
            person_page,
            answer_log,
            claim_queue,
            freshness,
            resume_snapshot,
            plan_state,
            card_queue,
            reply_slo: ReplySloProjection::default(),
            break_glass: BreakGlassProjection::from_channels(channels),
            built_at,
            lineage,
        },
        pending_item_commit: None,
    };
    materialized.validate()?;

    let expected_item_count = person_message_count
        .checked_add(reply_slo_count)
        .and_then(|count| count.checked_add(person_slide_count))
        .and_then(|count| count.checked_add(identity_event_count))
        .and_then(|count| {
            count.checked_add(u64::try_from(materialized.person_components.len()).ok()?)
        })
        .and_then(|count| {
            count.checked_add(u64::try_from(materialized.snapshot.claim_queue.groups.len()).ok()?)
        })
        .and_then(|count| {
            count.checked_add(u64::try_from(materialized.snapshot.card_queue.cards.len()).ok()?)
        })
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "projection item count overflow after paged rebuild".to_owned(),
            )
        })?;
    let staged_reply_slo_count =
        persistence.projection_item_count_by_owner(&staging_projection, REPLY_SLO_ITEM_OWNER)?;
    if staged_reply_slo_count != reply_slo_count {
        return Err(SelfHostError::Ingestion(format!(
            "paged rebuild staged {staged_reply_slo_count} reply SLO rows, expected {reply_slo_count}"
        )));
    }
    persistence.publish_projection_items_from_staging(
        &ProjectionRef::new("proj:person-page"),
        &staging_projection,
        &materialized.manifest_value()?,
        expected_item_count,
    )?;
    Ok(materialized)
}

fn apply_supplemental_delta(
    core: &mut AppCore,
    persistence: &dyn StoragePorts,
    changed: &lethe_core::domain::SupplementalRecord,
    built_at: DateTime<Utc>,
) -> Result<ProjectionItemCommit, SelfHostError> {
    let persisted_stats = persistence.observation_stats()?;
    if persisted_stats != core.observation_stats {
        return Err(SelfHostError::ProjectionStale(format!(
            "proj:person-page canonical watermark is {}/{}, but storage is {}/{}",
            core.observation_stats.count,
            core.observation_stats.max_append_seq,
            persisted_stats.count,
            persisted_stats.max_append_seq
        )));
    }
    let persisted_manifest = persistence
        .projection_records(&ProjectionRef::new("proj:person-page"))?
        .ok_or_else(|| {
            SelfHostError::ProjectionStale(
                "proj:person-page manifest is missing during supplemental delta".to_owned(),
            )
        })?;
    let persisted_materialized: MaterializedProjectionManifest =
        serde_json::from_value(persisted_manifest)?;
    if persisted_materialized.format_version != NON_CORPUS_MATERIALIZATION_VERSION
        || persisted_materialized.last_append_seq != persisted_stats.max_append_seq
        || persisted_materialized.observation_count != persisted_stats.count
        || persisted_materialized.supplemental_fingerprint != core.supplemental_fingerprint
        || persisted_materialized.canonical_observation_fingerprint
            != core.canonical_observation_fingerprint
        || persisted_materialized.identity_event_count != core.identity_event_count
        || persisted_materialized.person_component_count
            != u64::try_from(core.person_components.len()).map_err(|_| {
                SelfHostError::Ingestion("person component count does not fit u64".to_owned())
            })?
        || persisted_materialized.person_slide_count != core.person_slide_count
        || persisted_materialized.person_message_count != core.person_message_count
        || persisted_materialized.reply_slo_count != core.reply_slo_count
    {
        return Err(SelfHostError::ProjectionStale(
            "proj:person-page persisted manifest diverged from resident state".to_owned(),
        ));
    }
    if !core.snapshot.person_page.messages.is_empty()
        || !core.snapshot.reply_slo.rows.is_empty()
        || !core.snapshot.reply_slo.overdue.is_empty()
    {
        return Err(SelfHostError::Ingestion(
            "supplemental delta received resident row-store projection data".to_owned(),
        ));
    }
    if core.supplemental.get(&changed.id).is_none() {
        return Err(SelfHostError::Ingestion(format!(
            "supplemental {} is absent from resident state during delta materialization",
            changed.id
        )));
    }
    if core.supplemental_projection_cache.count() != core.supplemental_count {
        return Err(SelfHostError::Ingestion(
            "resident supplemental reducer count diverged from supplemental store".to_owned(),
        ));
    }
    let next_supplemental_fingerprint = core.resident_supplemental_fingerprint.clone();
    if next_supplemental_fingerprint == core.supplemental_fingerprint {
        return Err(SelfHostError::Ingestion(format!(
            "supplemental {} did not advance the materialized fingerprint",
            changed.id
        )));
    }

    core.snapshot.freshness = freshness_projection_after_delta(
        &core.snapshot.freshness,
        &core.freshness_thresholds,
        &[],
        built_at,
    )?;
    if affects_claim_queue(&changed.kind) {
        core.snapshot.claim_queue = core.supplemental_projection_cache.claim_queue();
    }
    (core.snapshot.resume_snapshot, core.snapshot.plan_state) = core
        .supplemental_projection_cache
        .cognition(&core.snapshot.claim_queue, built_at);
    core.snapshot.card_queue = core
        .supplemental_projection_cache
        .card_queue
        .projection(built_at);

    let mut inserts = Vec::new();
    let mut updates = Vec::new();
    let mut deletes = Vec::new();
    if let Some((person_id, created_at, frontend_profile)) =
        frontend_profile_from_supplemental_delta(
            persistence,
            &core.compact_state,
            changed,
            core.observation_stats.max_append_seq,
        )?
        && core
            .person_components
            .get(&person_id)
            .is_some_and(|component| component.consent != ConsentStatus::OptedOut)
    {
        let component = core.person_components.get_mut(&person_id).ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "frontend profile resolved to missing person component {person_id}"
            ))
        })?;
        let candidate_rank = FrontendProfileRank {
            richness_score: frontend_profile.profile.richness_score(),
            created_at,
            stable_id: frontend_profile.source_document_id.clone(),
        };
        let replace = component
            .frontend_profile_rank
            .as_ref()
            .is_none_or(|current| {
                (
                    current.richness_score,
                    current.created_at,
                    current.stable_id.as_str(),
                ) < (
                    candidate_rank.richness_score,
                    candidate_rank.created_at,
                    candidate_rank.stable_id.as_str(),
                )
            });
        if replace {
            let profile = component.profile.as_mut().ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "visible person component {person_id} has no profile"
                ))
            })?;
            profile.self_intro_text = frontend_profile.profile.bio_text.clone();
            profile.self_intro_slide_id = Some(frontend_profile.source_document_id.clone());
            profile.self_intro_thumbnail = frontend_profile
                .thumbnail_url
                .clone()
                .or_else(|| frontend_profile.thumbnail_ref.clone());
            profile.profile_updated_at = profile
                .last_activity
                .into_iter()
                .chain(Some(created_at))
                .max()
                .unwrap_or(created_at);
            profile.frontend_profile = Some(frontend_profile.clone());
            profile.frontend_profile_created_at = Some(created_at);
            component.frontend_profile_rank = Some(candidate_rank);
            component.frontend_profile = Some(frontend_profile);
            updates.push(person_component_projection_item(component)?);
        }
    }
    if changed.kind == "send-record@1"
        && let Some(draft_id) = changed.derived_from.supplementals.first()
        && let Some(draft) = core
            .supplemental_projection_cache
            .card_queue
            .draft(draft_id)
        && let Some(observation_id) = draft.derived_from.observations.first()
    {
        let stored = persistence
            .observation_by_id(observation_id)?
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "ReplySLO supplemental {} references missing observation {observation_id}",
                    changed.id
                ))
            })?;
        if stored.append_seq > core.observation_stats.max_append_seq {
            return Err(SelfHostError::Ingestion(format!(
                "ReplySLO supplemental {} crossed canonical high-water {}",
                changed.id, core.observation_stats.max_append_seq
            )));
        }
        let projected = core
            .communication_projection
            .project_observations(std::slice::from_ref(&stored.observation), built_at);
        if let Some(row) = projected.rows.into_iter().next() {
            let desired = reply_slo_projection_item(&row)?;
            let existing = persistence
                .projection_item_by_key(&ProjectionRef::new("proj:person-page"), &desired.item_key)?
                .ok_or_else(|| {
                    SelfHostError::Ingestion(format!(
                        "reply SLO row {} is missing during send-record delta",
                        desired.item_key
                    ))
                })?;
            reply_slo_from_projection_item(&existing)?;
            if existing != desired {
                updates.push(desired);
            }
        }
    }

    let queue_items =
        queue_projection_items(&core.snapshot.claim_queue, &core.snapshot.card_queue)?;
    let queue_keys = queue_items
        .iter()
        .map(|item| item.item_key.clone())
        .collect::<HashSet<_>>();
    for item in queue_items {
        match persistence
            .projection_item_by_key(&ProjectionRef::new("proj:person-page"), &item.item_key)?
        {
            Some(existing) if existing == item => {}
            Some(_) => updates.push(item),
            None => inserts.push(item),
        }
    }
    for owner in [CLAIM_QUEUE_ITEM_OWNER, CARD_QUEUE_ITEM_OWNER] {
        for item in
            persistence.projection_items_by_owner(&ProjectionRef::new("proj:person-page"), owner)?
        {
            if !queue_keys.contains(&item.item_key) {
                deletes.push(item.item_key);
            }
        }
    }

    core.snapshot.identity = IdentityResolutionOutput::default();
    core.snapshot.person_page = PersonPageOutput::default();
    core.snapshot.built_at = built_at;
    core.snapshot.lineage = build_person_page_lineage(
        &core.canonical_observation_fingerprint,
        core.observation_stats,
        &next_supplemental_fingerprint,
        core.supplemental_count,
        core.snapshot.lineage.output_count,
        built_at,
    );
    core.supplemental_fingerprint = next_supplemental_fingerprint;
    core.claim_queue_dirty = false;
    let item_commit = ProjectionItemCommit::Delta {
        inserts,
        updates,
        deletes,
    };
    item_commit.validate()?;
    Ok(item_commit)
}

fn frontend_profile_from_supplemental_delta(
    persistence: &dyn StoragePorts,
    compact_state: &CompactProjectionState,
    changed: &lethe_core::domain::SupplementalRecord,
    canonical_high_water: u64,
) -> Result<Option<(String, DateTime<Utc>, FrontendProfile)>, SelfHostError> {
    if changed.kind != "slide-analysis" {
        return Ok(None);
    }
    let Some(observation_id) = changed.derived_from.observations.first() else {
        return Ok(None);
    };
    let stored = persistence
        .observation_by_id(observation_id)?
        .ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "slide-analysis supplemental {} references missing observation {observation_id}",
                changed.id
            ))
        })?;
    if stored.append_seq > canonical_high_water {
        return Err(SelfHostError::Ingestion(format!(
            "slide-analysis supplemental {} crossed canonical high-water {canonical_high_water}",
            changed.id
        )));
    }
    let mut identifiers = BTreeSet::new();
    if let Ok(mut profile) = serde_json::from_value::<StudentProfile>(changed.payload.clone()) {
        profile.normalize_in_place();
        identifiers.extend(profile.email);
        identifiers.extend(profile.generated_email);
    }
    if let Some(owner) = stored
        .observation
        .payload
        .pointer("/relations/owner")
        .and_then(serde_json::Value::as_str)
    {
        identifiers.insert(owner.to_owned());
    }
    if let Some(editors) = stored
        .observation
        .payload
        .pointer("/relations/editors")
        .and_then(serde_json::Value::as_array)
    {
        identifiers.extend(
            editors
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
        );
    }
    let mut person_ids = BTreeSet::new();
    for identifier in identifiers {
        if let Some(nodes) = compact_state.nodes_by_identifier_value.get(&identifier) {
            for node_id in nodes {
                person_ids.insert(compact_state.person_id_for_node(*node_id)?);
            }
        }
    }
    let identity = IdentityResolutionOutput {
        resolved_persons: person_ids
            .iter()
            .filter_map(|person_id| compact_state.identity.resolved_person(person_id, "1.0.0"))
            .collect(),
        candidates: Vec::new(),
        person_identifiers: Vec::new(),
    };
    let mut projected = PersonPageProjector::project_frontend_profiles(
        &identity,
        std::slice::from_ref(&stored.observation),
        &[changed],
    )
    .into_iter()
    .collect::<Vec<_>>();
    projected.sort_by(|left, right| left.0.cmp(&right.0));
    if projected.len() > 1 {
        return Err(SelfHostError::Ingestion(format!(
            "slide-analysis supplemental {} resolved to multiple person components",
            changed.id
        )));
    }
    Ok(projected
        .pop()
        .map(|(person_id, (created_at, profile))| (person_id, created_at, profile)))
}

fn current_materialized_snapshot(
    persistence: &dyn StoragePorts,
    value: serde_json::Value,
    stats: ObservationStats,
    supplemental_fingerprint: &str,
    persisted_projection_item_count: u64,
    persisted_reply_slo_count: u64,
) -> Result<MaterializedSnapshotRestore, SelfHostError> {
    let object = value.as_object().ok_or_else(|| {
        SelfHostError::Ingestion(
            "proj:person-page materialization manifest is not a JSON object".to_owned(),
        )
    })?;
    let Some(raw_format_version) = object.get("format_version") else {
        return Ok(MaterializedSnapshotRestore::RebuildRequired {
            reason: "persisted legacy manifest has no format_version".to_owned(),
        });
    };
    let Some(format_version) = raw_format_version.as_u64() else {
        return Ok(MaterializedSnapshotRestore::RebuildRequired {
            reason: "persisted legacy manifest format_version is not an unsigned integer"
                .to_owned(),
        });
    };
    let current_format_version = u64::from(NON_CORPUS_MATERIALIZATION_VERSION);
    if format_version < current_format_version {
        return Ok(MaterializedSnapshotRestore::RebuildRequired {
            reason: format!(
                "persisted format {format_version} is older than current format {current_format_version}"
            ),
        });
    }
    if format_version > current_format_version {
        return Err(SelfHostError::Ingestion(format!(
            "proj:person-page materialization format {format_version} is newer than supported format {current_format_version}"
        )));
    }
    let manifest: MaterializedProjectionManifest = serde_json::from_value(value)?;
    if manifest.last_append_seq != stats.max_append_seq || manifest.observation_count != stats.count
    {
        return Ok(MaterializedSnapshotRestore::RebuildRequired {
            reason: format!(
                "persisted canonical watermark count/append_seq={}/{} differs from storage {}/{}",
                manifest.observation_count,
                manifest.last_append_seq,
                stats.count,
                stats.max_append_seq
            ),
        });
    }
    if manifest.supplemental_fingerprint != supplemental_fingerprint {
        return Ok(MaterializedSnapshotRestore::RebuildRequired {
            reason: format!(
                "persisted supplemental fingerprint {} differs from current {}",
                manifest.supplemental_fingerprint, supplemental_fingerprint
            ),
        });
    }
    let base_projection_item_count = manifest
        .person_message_count
        .checked_add(manifest.reply_slo_count)
        .and_then(|count| count.checked_add(manifest.person_slide_count))
        .and_then(|count| count.checked_add(manifest.identity_event_count))
        .and_then(|count| count.checked_add(manifest.person_component_count))
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "proj:person-page manifest projection item count overflow".to_owned(),
            )
        })?;
    let queue_projection_item_count = manifest
        .snapshot
        .claim_queue
        .groups
        .len()
        .checked_add(manifest.snapshot.card_queue.cards.len())
        .and_then(|count| u64::try_from(count).ok())
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "proj:person-page queue projection item count overflow".to_owned(),
            )
        })?;
    let expected_projection_item_count =
        if persisted_projection_item_count == base_projection_item_count {
            base_projection_item_count
        } else {
            base_projection_item_count
                .checked_add(queue_projection_item_count)
                .ok_or_else(|| {
                    SelfHostError::Ingestion(
                        "proj:person-page projection item count overflow".to_owned(),
                    )
                })?
        };
    if expected_projection_item_count != persisted_projection_item_count {
        return Err(SelfHostError::Ingestion(format!(
            "proj:person-page manifest expects {expected_projection_item_count} projection item rows, but storage contains {persisted_projection_item_count}"
        )));
    }
    if manifest.reply_slo_count != persisted_reply_slo_count {
        return Err(SelfHostError::Ingestion(format!(
            "proj:person-page manifest expects {} reply SLO rows, but reserved owner contains {persisted_reply_slo_count}",
            manifest.reply_slo_count
        )));
    }
    let identity_items = persistence.projection_items_by_owner(
        &ProjectionRef::new("proj:person-page"),
        IDENTITY_EVENT_ITEM_OWNER,
    )?;
    if u64::try_from(identity_items.len()).ok() != Some(manifest.identity_event_count) {
        return Err(SelfHostError::Ingestion(format!(
            "proj:person-page manifest expects {} identity events, but storage contains {}",
            manifest.identity_event_count,
            identity_items.len()
        )));
    }
    let mut compact_state = CompactProjectionState {
        identity: IdentityState::default(),
        observation_ids_by_node: Vec::new(),
        nodes_by_observation_id: BTreeMap::new(),
        nodes_by_identifier_value: BTreeMap::new(),
        consent_by_subject: BTreeMap::new(),
        consent_by_identifier: BTreeMap::new(),
    };
    let mut previous_append_seq = None;
    for item in &identity_items {
        let event = identity_replay_event_from_projection_item(item)?;
        if previous_append_seq.is_some_and(|previous| previous >= event.append_seq) {
            return Err(SelfHostError::Ingestion(
                "persisted identity replay events are not in strict append order".to_owned(),
            ));
        }
        previous_append_seq = Some(event.append_seq);
        compact_state.apply_replay_event(&event)?;
    }
    compact_state.validate()?;

    let component_items = persistence.projection_items_by_owner(
        &ProjectionRef::new("proj:person-page"),
        PERSON_COMPONENT_ITEM_OWNER,
    )?;
    if u64::try_from(component_items.len()).ok() != Some(manifest.person_component_count) {
        return Err(SelfHostError::Ingestion(format!(
            "proj:person-page manifest expects {} component aggregates, but storage contains {}",
            manifest.person_component_count,
            component_items.len()
        )));
    }
    let mut person_components = BTreeMap::new();
    for item in &component_items {
        let component = person_component_from_projection_item(item)?;
        let person_id = component.person.person_id.as_str().to_owned();
        if person_components
            .insert(person_id.clone(), component)
            .is_some()
        {
            return Err(SelfHostError::Ingestion(format!(
                "persisted person component {person_id} is duplicated"
            )));
        }
    }
    let identity = compact_state.resolve_identity();
    let person_consents = compact_state.person_consents(&identity);
    for person in &identity.resolved_persons {
        let person_id = person.person_id.as_str();
        let component = person_components.get(person_id).ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "identity component {person_id} has no keyed aggregate"
            ))
        })?;
        if serde_json::to_value(&component.person)? != serde_json::to_value(person)?
            || person_consents.get(person_id) != Some(&component.consent)
        {
            return Err(SelfHostError::Ingestion(format!(
                "keyed person component {person_id} disagrees with identity replay"
            )));
        }
    }
    let person_page = PersonPageOutput {
        profiles: person_components
            .values()
            .filter_map(|component| component.profile.clone())
            .collect(),
        slides: Vec::new(),
        messages: Vec::new(),
        activities: person_components
            .values()
            .filter_map(|component| component.activity.clone())
            .collect(),
    };
    let communication_projection = manifest.communication_projection;
    let auxiliary = manifest.snapshot;
    let materialized = MaterializedProjectionSnapshot {
        format_version: manifest.format_version,
        last_append_seq: manifest.last_append_seq,
        observation_count: manifest.observation_count,
        canonical_observation_fingerprint: manifest.canonical_observation_fingerprint,
        supplemental_fingerprint: manifest.supplemental_fingerprint,
        compact_state,
        person_consents,
        person_components,
        identity_event_count: manifest.identity_event_count,
        person_slide_count: manifest.person_slide_count,
        person_message_count: manifest.person_message_count,
        reply_slo_count: manifest.reply_slo_count,
        communication_projection,
        snapshot: ProjectionSnapshot {
            identity,
            person_page,
            answer_log: auxiliary.answer_log,
            claim_queue: auxiliary.claim_queue,
            freshness: auxiliary.freshness,
            resume_snapshot: auxiliary.resume_snapshot,
            plan_state: auxiliary.plan_state,
            card_queue: auxiliary.card_queue,
            reply_slo: ReplySloProjection::default(),
            break_glass: auxiliary.break_glass,
            built_at: auxiliary.built_at,
            lineage: auxiliary.lineage,
        },
        pending_item_commit: None,
    };
    materialized.validate()?;
    Ok(MaterializedSnapshotRestore::Restored(materialized))
}

#[derive(Debug)]
enum MaterializedSnapshotRestore {
    Restored(MaterializedProjectionSnapshot),
    RebuildRequired { reason: String },
}

fn validate_persisted_supplemental_anchors(
    persistence: &SqlitePersistence,
    records: &[lethe_core::domain::SupplementalRecord],
) -> Result<(), SelfHostError> {
    for record in records {
        for observation_id in &record.derived_from.observations {
            if persistence.observation_by_id(observation_id)?.is_none() {
                return Err(SelfHostError::Ingestion(format!(
                    "persisted supplemental {} references missing observation {}",
                    record.id, observation_id
                )));
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct SlackSourceRuntime {
    config: SlackConfig,
    client: HttpSlackClient,
    replies_client: HttpSlackClient,
}

#[derive(Clone)]
struct GoogleSourceRuntime {
    config: GoogleConfig,
    client: HttpGoogleSlidesClient,
}

fn bootstrap_materialized_placeholder(
    stats: ObservationStats,
    supplementals: &[SupplementalRecord],
    channels: &[lethe_registry::registry::ChannelRecord],
) -> Result<MaterializedProjectionSnapshot, SelfHostError> {
    let cache = SupplementalProjectionCache::from_records(supplementals);
    let claim_queue = cache.claim_queue();
    let (resume_snapshot, plan_state) = cache.cognition(&claim_queue, Utc::now());
    let built_at = Utc::now();
    let canonical_fingerprint = hex::encode([0_u8; 32]);
    let supplemental_fingerprint = supplemental_fingerprint(supplementals)?;
    let snapshot = ProjectionSnapshot {
        claim_queue,
        resume_snapshot,
        plan_state,
        card_queue: cache.card_queue.projection(built_at),
        break_glass: BreakGlassProjection::from_channels(channels),
        built_at,
        lineage: build_person_page_lineage(
            &canonical_fingerprint,
            stats,
            &supplemental_fingerprint,
            supplementals.len(),
            0,
            built_at,
        ),
        ..ProjectionSnapshot::default()
    };
    let materialized = MaterializedProjectionSnapshot {
        format_version: NON_CORPUS_MATERIALIZATION_VERSION,
        last_append_seq: stats.max_append_seq,
        observation_count: stats.count,
        canonical_observation_fingerprint: canonical_fingerprint,
        supplemental_fingerprint,
        compact_state: CompactProjectionState::build(&[])?,
        person_consents: BTreeMap::new(),
        person_components: BTreeMap::new(),
        identity_event_count: 0,
        person_slide_count: 0,
        person_message_count: 0,
        reply_slo_count: 0,
        communication_projection: CommunicationProjectionState::default(),
        snapshot,
        pending_item_commit: None,
    };
    materialized.validate()?;
    Ok(materialized)
}

impl AppService {
    pub fn bootstrap(config: SelfHostConfig) -> Result<Self, SelfHostError> {
        let open_operational_ledger =
            || -> Result<Box<dyn OperationalStoragePorts>, SelfHostError> {
                match &config.operational_ledger {
                    OperationalLedgerConfig::Sqlite {
                        data_space_id,
                        database_path,
                        blob_dir,
                        secret_encryption_key,
                    } => Ok(Box::new(
                        SqliteOperationalEventStore::open(
                            data_space_id.clone(),
                            database_path,
                            blob_dir,
                            secret_encryption_key,
                        )
                        .map_err(|error| SelfHostError::OperationalLedger(error.to_string()))?,
                    )),
                    OperationalLedgerConfig::Postgres {
                        data_space_id,
                        dsn,
                        schema,
                        role,
                    } => Ok(Box::new(
                        PostgresOperationalEventStore::connect_no_tls(
                            data_space_id.clone(),
                            dsn.expose(),
                            schema,
                            role,
                        )
                        .map_err(|error| SelfHostError::OperationalLedger(error.to_string()))?,
                    )),
                }
            };
        let operational_ledger = open_operational_ledger()?;
        // The history read API is a projection API.  Build its immutable snapshot before
        // accepting requests so a first owner query cannot hold the operational-ledger mutex
        // while scanning every historical event.
        let history_projection = HistoryProjection::rebuild(operational_ledger.as_ref())?;
        let persistence = SqlitePersistence::open_with_routing_key_order(
            &config.database_path,
            &config.blob_dir,
            &config.secret_encryption_key,
            config.routing_key_order,
        )?;
        let schema_migrations_applied = persistence.schema_migrations_applied_on_open();
        let stats = persistence.observation_stats()?;
        let supplementals = persistence.load_supplementals()?;
        validate_persisted_supplemental_anchors(&persistence, &supplementals)?;
        let supplemental_fingerprint = supplemental_fingerprint(&supplementals)?;
        let person_page_ref = ProjectionRef::new("proj:person-page");
        let persisted_projection_item_count =
            persistence.projection_item_count(&person_page_ref)?;
        let persisted_reply_slo_count =
            persistence.projection_item_count_by_owner(&person_page_ref, REPLY_SLO_ITEM_OWNER)?;
        let persisted_manifest = persistence.projection_records(&person_page_ref)?;
        let had_persisted_manifest = persisted_manifest.is_some();
        let persisted_materialized = match persisted_manifest {
            Some(value) => match current_materialized_snapshot(
                &persistence,
                value,
                stats,
                &supplemental_fingerprint,
                persisted_projection_item_count,
                persisted_reply_slo_count,
            )? {
                MaterializedSnapshotRestore::Restored(materialized) => Some(materialized),
                MaterializedSnapshotRestore::RebuildRequired { reason } => {
                    tracing::warn!(
                        manifest_restore_rejection_reason = %reason,
                        "persisted non-corpus materialization restore rejected"
                    );
                    None
                }
            },
            None => None,
        };
        let persisted_queue_item_count = persistence
            .projection_item_count_by_owner(&person_page_ref, CLAIM_QUEUE_ITEM_OWNER)?
            .checked_add(
                persistence
                    .projection_item_count_by_owner(&person_page_ref, CARD_QUEUE_ITEM_OWNER)?,
            )
            .ok_or_else(|| {
                SelfHostError::Ingestion("queue projection item count overflow".to_owned())
            })?;
        let queue_index_missing = persisted_materialized.as_ref().is_some_and(|materialized| {
            let expected = materialized.snapshot.claim_queue.groups.len()
                + materialized.snapshot.card_queue.cards.len();
            u64::try_from(expected).is_ok_and(|expected| persisted_queue_item_count < expected)
        });
        if queue_index_missing {
            tracing::warn!(
                manifest_restore_rejection_reason =
                    "persisted queue projection item index is incomplete",
                "persisted non-corpus materialization restore requires recovery"
            );
        }
        let requires_background_rebuild =
            schema_migrations_applied || persisted_materialized.is_none() || queue_index_missing;
        let materialized = match persisted_materialized {
            Some(materialized) => materialized,
            None => bootstrap_materialized_placeholder(stats, &supplementals, &config.channels)?,
        };
        let persisted_sync_state = persistence.load_sync_state("all")?;
        let core = AppCore::from_materialized_with_sync_state(
            materialized,
            Vec::new(),
            supplementals,
            freshness_thresholds(&config),
            config.channels.clone(),
            persisted_sync_state,
        )?;
        let mut persistence_read_pool = Vec::with_capacity(4);
        persistence_read_pool.push(Arc::new(Mutex::new(
            Box::new(persistence) as Box<dyn StoragePorts>
        )));
        for _ in 1..4 {
            persistence_read_pool.push(Arc::new(Mutex::new(Box::new(
                SqlitePersistence::open_with_routing_key_order(
                    &config.database_path,
                    &config.blob_dir,
                    &config.secret_encryption_key,
                    config.routing_key_order,
                )?,
            )
                as Box<dyn StoragePorts>)));
        }
        let persistence: Arc<Mutex<Box<dyn StoragePorts>>> = Arc::new(Mutex::new(Box::new(
            SqlitePersistence::open_with_routing_key_order(
                &config.database_path,
                &config.blob_dir,
                &config.secret_encryption_key,
                config.routing_key_order,
            )?,
        )));
        let mut operational_ledger_read_pool = Vec::with_capacity(4);
        for _ in 0..4 {
            operational_ledger_read_pool.push(Arc::new(Mutex::new(open_operational_ledger()?)));
        }
        let corpus_config = config.corpus.projector_config();
        let search_index = search_index::SearchIndexManager::bootstrap(
            lethe_search_index::IndexRoot::new(
                &config.corpus.index_dir,
                config.corpus.writer_heap_bytes,
                corpus_config.fingerprint(),
            )?,
            CorpusProjector::new(corpus_config),
            config.corpus.rebuild_page_size,
            Arc::clone(&persistence_read_pool[0]),
        );
        let search_job_queue =
            start_search_job_workers(config.resource_limits.max_search_job_workers)?;
        let slack_sources = config
            .slack_sources
            .iter()
            .cloned()
            .map(|source| {
                Ok(SlackSourceRuntime {
                    client: HttpSlackClient::new(source.bot_token.expose().to_owned())?,
                    replies_client: HttpSlackClient::new(source.thread_token.expose().to_owned())?,
                    config: source,
                })
            })
            .collect::<Result<Vec<_>, AdapterError>>()?;
        let google_sources = config
            .google_sources
            .iter()
            .cloned()
            .map(|source| {
                Ok(GoogleSourceRuntime {
                    client: HttpGoogleSlidesClient::new(&source)?,
                    config: source,
                })
            })
            .collect::<Result<Vec<_>, AdapterError>>()?;
        let slide_analyzer = config
            .slide_ai
            .as_ref()
            .map(|slide_ai| GeminiSlideAnalyzer::new(slide_ai.api_key.expose(), &slide_ai.model))
            .transpose()?;

        let core_snapshot = Arc::new(ArcSwap::from_pointee(core.clone()));
        let service = Self {
            core: Arc::new(Mutex::new(core)),
            core_snapshot,
            persistence,
            persistence_read_pool,
            persistence_read_next: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            operational_ledger: Arc::new(Mutex::new(operational_ledger)),
            operational_ledger_read_pool,
            operational_ledger_read_next: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            history_projection: Arc::new(Mutex::new(history_projection)),
            derived_projection_lane: Arc::new(Mutex::new(())),
            bulk_import_operation: Arc::new(Mutex::new(())),
            import_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            search_index,
            search_jobs: Arc::new(Mutex::new(BTreeMap::new())),
            search_job_sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            search_job_queue,
            config: Arc::new(config),
            slack_sources,
            google_sources,
            slide_analyzer,
            resilient_executor: Arc::new(ResilientExecutor::new(
                3,
                std::time::Duration::from_secs(60),
            )),
            append_consumer_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            append_consumer_error: Arc::new(Mutex::new(None)),
            search_index_catch_up_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            non_corpus_rebuild_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            non_corpus_rebuild_error: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            non_corpus_rebuild_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            non_corpus_rebuild_reasons: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            non_corpus_rebuild_page_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            non_corpus_rebuild_page_delay: None,
            #[cfg(test)]
            publish_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            search_job_test_gate: None,
            #[cfg(test)]
            search_job_test_fault: None,
        };
        if requires_background_rebuild {
            let _derived_lane = service
                .derived_projection_lane
                .lock()
                .map_err(|_| SelfHostError::LockPoisoned)?;
            let mut core = service.core_lock()?;
            core.mark_non_corpus_materializations_stale();
            service.publish_core_snapshot(&core);
            drop(core);
            drop(_derived_lane);
            let mut core = service.core_lock()?;
            service.refresh_materialized_snapshot_with_reason(
                &mut core,
                if !had_persisted_manifest {
                    "bootstrap"
                } else if schema_migrations_applied {
                    "migration"
                } else {
                    "recovery"
                },
            )?;
        }
        let cursor_initialized = service
            .persistence_read_lock()?
            .get_state("append_consumer:person-page")?
            .is_some();
        if !cursor_initialized {
            let current_high_water = service
                .persistence_read_lock()?
                .observation_stats()?
                .max_append_seq;
            service.persistence_lock()?.set_state(
                "append_consumer:person-page",
                &current_high_water.to_string(),
            )?;
        }
        service.catch_up_identity_bridge()?;
        Ok(service)
    }

    pub fn append_operational_events(
        &self,
        requests: &[OperationalAppendRequest],
    ) -> Result<Vec<OperationalAppendOutcome>, SelfHostError> {
        self.operational_ledger
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?
            .append_operational_events(requests)
            .map_err(Into::into)
    }

    pub fn operational_event_stats(&self) -> Result<OperationalEventStats, SelfHostError> {
        self.operational_ledger_read_lock()?
            .operational_event_stats()
            .map_err(Into::into)
    }

    pub fn operational_event_page(
        &self,
        after_cursor: u64,
        limit: usize,
    ) -> Result<Vec<StoredOperationalEvent>, SelfHostError> {
        self.operational_ledger_read_lock()?
            .operational_event_page(after_cursor, limit)
            .map_err(Into::into)
    }

    pub fn operational_events_by_filter(
        &self,
        filter: &OperationalEventFilter,
        after_cursor: u64,
        limit: usize,
    ) -> Result<Vec<StoredOperationalEvent>, SelfHostError> {
        self.operational_ledger_read_lock()?
            .operational_events_by_filter(filter, after_cursor, limit)
            .map_err(Into::into)
    }

    pub fn validate_page_limit(&self, limit: usize, resource: &str) -> Result<(), SelfHostError> {
        if limit == 0 || limit > self.config.resource_limits.max_page_size {
            return Err(SelfHostError::IngestionRequest {
                code: "page_limit_exceeded",
                detail: format!(
                    "{resource} page limit {limit} must be between 1 and {}",
                    self.config.resource_limits.max_page_size
                ),
                details: serde_json::json!({
                    "resource": resource,
                    "actual": limit,
                    "maximum": self.config.resource_limits.max_page_size,
                }),
            });
        }
        Ok(())
    }

    pub fn max_page_size(&self) -> usize {
        self.config.resource_limits.max_page_size
    }

    pub fn validate_operational_page_limit(&self, limit: usize) -> Result<(), SelfHostError> {
        self.validate_page_limit(limit, "operational event")
    }

    pub fn operational_events_for_stream(
        &self,
        stream_id: &str,
        after_stream_version: u64,
        limit: usize,
    ) -> Result<Vec<StoredOperationalEvent>, SelfHostError> {
        self.operational_ledger_read_lock()?
            .operational_events_for_stream(stream_id, after_stream_version, limit)
            .map_err(Into::into)
    }

    pub fn operational_event_by_id(
        &self,
        event_id: &lethe_core::domain::OperationalEventId,
    ) -> Result<Option<StoredOperationalEvent>, SelfHostError> {
        self.operational_ledger_read_lock()?
            .operational_event_by_id(event_id)
            .map_err(Into::into)
    }

    pub fn put_operational_blob(&self, data: &[u8]) -> Result<BlobRef, SelfHostError> {
        self.operational_ledger
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?
            .put_blob(data, self.config.resource_limits.max_blob_bytes)
            .map_err(Into::into)
    }

    pub fn operational_blob_body_limit(&self) -> usize {
        self.config.resource_limits.max_blob_bytes
    }

    pub fn get_operational_blob(
        &self,
        blob_ref: &BlobRef,
    ) -> Result<Option<Vec<u8>>, SelfHostError> {
        self.operational_ledger_read_lock()?
            .get_blob(blob_ref)
            .map_err(Into::into)
    }

    pub fn inventory_history(
        &self,
        request: &HistoryInventoryRequest,
    ) -> Result<HistoryInventoryReport, SelfHostError> {
        let report = lethe_history::inventory_history(request)?;
        Ok(report)
    }

    pub fn import_history(
        &self,
        command: &HistoryImportCommand,
    ) -> Result<HistoryImportResult, SelfHostError> {
        let ledger = self
            .operational_ledger
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?;
        let result = lethe_history::import_history(
            ledger.as_ref(),
            command,
            self.config.resource_limits.max_blob_bytes,
        )?;
        Ok(result)
    }

    pub fn query_history(
        &self,
        request: &HistoryQueryRequest,
    ) -> Result<HistoryQueryResponse, SelfHostError> {
        if request.max_result_bytes > self.config.resource_limits.max_payload_bytes {
            return Err(SelfHostError::Ingestion(format!(
                "history max_result_bytes must be between 1 and {}",
                self.config.resource_limits.max_payload_bytes
            )));
        }
        let ledger = self.operational_ledger_read_lock()?;
        let mut projection = self
            .history_projection
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?;
        if projection.source_watermark() != ledger.operational_event_stats()?.max_cursor {
            projection.refresh_from(ledger.as_ref())?;
        }
        Ok(projection.query(ledger.as_ref(), request)?)
    }

    pub fn spawn_polling_task(&self) {
        let service = self.clone();
        let interval = self.config.poll_interval;
        tokio::spawn(async move {
            loop {
                let cloned = service.clone();
                let result = tokio::task::spawn_blocking(move || cloned.sync_all()).await;
                if let Err(err) = result {
                    tracing::error!(error = %err, "poll task join failed");
                } else if let Ok(Err(err)) = result {
                    tracing::error!(error = %err, "poll sync failed");
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    pub fn authorize_headers(
        &self,
        headers: &HeaderMap,
        required_scope: &str,
    ) -> Result<(), SelfHostError> {
        self.authorize_headers_all(headers, &[required_scope])
    }

    pub fn authorize_headers_all(
        &self,
        headers: &HeaderMap,
        required_scopes: &[&str],
    ) -> Result<(), SelfHostError> {
        if required_scopes.is_empty() {
            return Err(SelfHostError::Policy(
                "at least one required scope must be specified".to_string(),
            ));
        }
        let Some(header) = headers.get(axum::http::header::AUTHORIZATION) else {
            self.emit_audit(
                "actor:anonymous",
                AuditEventKind::PolicyDenial,
                serde_json::json!({ "required_scopes": required_scopes, "reason": "missing bearer token" }),
            )?;
            return Err(SelfHostError::Auth("missing bearer token".to_string()));
        };
        let raw = match header.to_str() {
            Ok(raw) => raw,
            Err(_) => {
                self.emit_audit(
                    "actor:anonymous",
                    AuditEventKind::PolicyDenial,
                    serde_json::json!({
                        "required_scopes": required_scopes,
                        "reason": "invalid authorization header"
                    }),
                )?;
                return Err(SelfHostError::Auth(
                    "invalid authorization header".to_string(),
                ));
            }
        };
        let token = match raw.strip_prefix("Bearer ") {
            Some(token) => token,
            None => {
                self.emit_audit(
                    "actor:anonymous",
                    AuditEventKind::PolicyDenial,
                    serde_json::json!({
                        "required_scopes": required_scopes,
                        "reason": "authorization must use Bearer token"
                    }),
                )?;
                return Err(SelfHostError::Auth(
                    "authorization must use Bearer token".to_string(),
                ));
            }
        };
        let matched = match self
            .config
            .api_tokens
            .iter()
            .find(|candidate| candidate.token.expose() == token)
        {
            Some(matched) => matched,
            None => {
                self.emit_audit(
                    "actor:anonymous",
                    AuditEventKind::PolicyDenial,
                    serde_json::json!({
                        "required_scopes": required_scopes,
                        "reason": "token rejected"
                    }),
                )?;
                return Err(SelfHostError::Auth("token rejected".to_string()));
            }
        };
        if required_scopes.iter().all(|required_scope| {
            matched
                .scopes
                .iter()
                .any(|scope| scope == "*" || scope == required_scope)
        }) {
            self.emit_audit(
                "actor:api-token",
                audit_kind_for_scope(required_scopes[0]),
                serde_json::json!({ "required_scopes": required_scopes }),
            )?;
            Ok(())
        } else {
            self.emit_audit(
                "actor:api-token",
                AuditEventKind::PolicyDenial,
                serde_json::json!({ "required_scopes": required_scopes, "reason": "scope denied" }),
            )?;
            Err(SelfHostError::Policy(format!(
                "token lacks required scopes {}",
                required_scopes.join(",")
            )))
        }
    }

    fn emit_audit(
        &self,
        actor: &str,
        kind: AuditEventKind,
        detail: serde_json::Value,
    ) -> Result<(), SelfHostError> {
        let event = self.build_audit_event(actor, kind, detail)?;
        let audit = AuditEventRecord {
            id: event.id.clone(),
            timestamp: event.timestamp.to_rfc3339(),
            actor: event.actor.as_str().to_owned(),
            event_json: serde_json::to_string(&event)?,
        };
        self.persistence_lock()?.record_audit_event(
            &audit.id,
            &audit.timestamp,
            &audit.actor,
            &audit.event_json,
        )?;
        Ok(())
    }

    pub(super) fn build_audit_event(
        &self,
        actor: &str,
        kind: AuditEventKind,
        detail: serde_json::Value,
    ) -> Result<AuditEvent, SelfHostError> {
        Ok(AuditEvent {
            id: format!("audit:{}", uuid::Uuid::now_v7()),
            timestamp: Utc::now(),
            actor: ActorRef::new(actor),
            kind,
            detail,
        })
    }

    pub(super) fn audit_record(event: &AuditEvent) -> Result<AuditEventRecord, SelfHostError> {
        Ok(AuditEventRecord {
            id: event.id.clone(),
            timestamp: event.timestamp.to_rfc3339(),
            actor: event.actor.as_str().to_owned(),
            event_json: serde_json::to_string(event)?,
        })
    }

    pub fn attribute_inventory_documents(
        &self,
    ) -> Result<Vec<AttributeInventoryDocument>, SelfHostError> {
        let core = self.core_snapshot();
        self.ensure_projection_fresh(&core.catalog, "proj:person-page")?;
        Ok(build_inventory_documents(&core.snapshot))
    }

    pub fn ingest_observation_drafts(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
    ) -> Result<ImportReport, SelfHostError> {
        self.ingest_observation_drafts_with_admission_generation(
            drafts,
            source_instance_id,
            None,
            None,
        )
    }

    pub fn ingest_observation_drafts_with_session(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: Option<&str>,
    ) -> Result<ImportReport, SelfHostError> {
        self.ingest_observation_drafts_with_admission_generation(
            drafts,
            source_instance_id,
            bulk_session_id,
            None,
        )
    }

    pub fn ingest_observation_drafts_with_admission_generation(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: Option<&str>,
        admission_generation: Option<u64>,
    ) -> Result<ImportReport, SelfHostError> {
        self.ingest_observation_drafts_with_admission_generation_and_spawn_wait(
            drafts,
            source_instance_id,
            bulk_session_id,
            admission_generation,
            Duration::ZERO,
        )
    }

    pub(super) fn ingest_observation_drafts_with_admission_generation_and_spawn_wait(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: Option<&str>,
        admission_generation: Option<u64>,
        spawn_blocking_wait: Duration,
    ) -> Result<ImportReport, SelfHostError> {
        let context = ObservationImportContext::from_drafts(&drafts);
        let bulk_session_requested = bulk_session_id.is_some();
        let mut timer = ObservationImportTimer::new();
        timer.record_stage(ImportTimingStage::SpawnBlockingWait, spawn_blocking_wait);
        let mut materialization_state = ImportMaterializationState::NotRun;
        let result = (|| {
            let _import_permit = self.try_acquire_import_permit()?;
            if source_instance_id.trim().is_empty() {
                return Err(SelfHostError::Ingestion(
                    "source_instance_id must not be blank".to_owned(),
                ));
            }
            if drafts.len() > self.config.resource_limits.max_import_drafts {
                return Err(SelfHostError::Ingestion(format!(
                    "draft count {} exceeds configured maximum {}",
                    drafts.len(),
                    self.config.resource_limits.max_import_drafts
                )));
            }
            self.enforce_cutover_admission_for_import(
                source_instance_id,
                CutoverApiVersion::V1,
                admission_generation,
                &mut timer,
            )?;

            let _operation = self.bulk_import_operation_lock_for_import(&mut timer)?;
            let core = self.core_snapshot();
            let bulk_session = self.bulk_import_session_for_append(bulk_session_id, &mut timer)?;

            let mut report = ImportReport {
                ingested: 0,
                duplicates: 0,
                quarantined: 0,
                rejected: 0,
                results: Vec::new(),
                summary: ImportSummary::default(),
            };

            let mut prepared_observations = Vec::new();
            let mut remaining = drafts.into_iter();
            loop {
                let batch = remaining
                    .by_ref()
                    .take(IMPORT_PROCESS_BATCH_SIZE)
                    .collect::<Vec<_>>();
                if batch.is_empty() {
                    break;
                }
                self.prepare_legacy_observation_draft_batch(
                    &core,
                    batch,
                    source_instance_id,
                    &mut timer,
                    &mut prepared_observations,
                )?;
            }

            let audit_events = if prepared_observations.is_empty() {
                Vec::new()
            } else {
                let event = self.build_audit_event(
                    "actor:self-host",
                    AuditEventKind::WriteExecution,
                    serde_json::json!({
                        "mode": "bulk_observation_import",
                        "source_instance_id": source_instance_id,
                        "requested": prepared_observations.len(),
                        "bulk_session_id": bulk_session_id,
                        "privacy_decisions": privacy_audit_details(&prepared_observations),
                    }),
                )?;
                vec![AppService::audit_record(&event)?]
            };
            let outcomes = if prepared_observations.is_empty() {
                Vec::new()
            } else {
                let persistence = self.persistence_lock_for_import(&mut timer)?;
                let stage_started_at = Instant::now();
                let append_result = persistence
                    .append_observations_v1_with_admission(
                        source_instance_id,
                        admission_generation,
                        &prepared_observations,
                        &audit_events,
                    )
                    .map_err(SelfHostError::Storage);
                timer.record_stage(ImportTimingStage::LedgerAppend, stage_started_at.elapsed());
                append_result?
            };
            if outcomes.len() != prepared_observations.len() {
                return Err(SelfHostError::Ingestion(format!(
                    "bulk append returned {} outcomes for {} observations",
                    outcomes.len(),
                    prepared_observations.len()
                )));
            }

            let mut request_appended_observations = Vec::new();
            for (observation, outcome) in prepared_observations.into_iter().zip(outcomes) {
                match outcome {
                    DurableAppendOutcome::Appended(id) => {
                        if id != observation.id {
                            return Err(SelfHostError::Ingestion(format!(
                                "bulk append returned observation id {id}, expected {}",
                                observation.id
                            )));
                        }
                        report.ingested += 1;
                        report.results.push(ImportItemResult {
                            client_ref: report.results.len().to_string(),
                            outcome: ImportOutcome::Ingested,
                            observation_id: Some(id),
                            existing_id: None,
                            ticket: None,
                            error_code: None,
                            failure_class: None,
                            reason: None,
                            details: None,
                        });
                        request_appended_observations.push(observation);
                    }
                    DurableAppendOutcome::Duplicate(existing_id) => {
                        report.duplicates += 1;
                        report.results.push(ImportItemResult {
                            client_ref: report.results.len().to_string(),
                            outcome: ImportOutcome::Duplicate,
                            observation_id: None,
                            existing_id: Some(existing_id),
                            ticket: None,
                            error_code: None,
                            failure_class: None,
                            reason: None,
                            details: None,
                        });
                    }
                    DurableAppendOutcome::CanonicalCollision(existing_id) => {
                        report.quarantined += 1;
                        let reason = format!(
                            "canonical identity collision with existing observation {existing_id}"
                        );
                        report.results.push(ImportItemResult {
                            client_ref: report.results.len().to_string(),
                            outcome: ImportOutcome::Quarantined,
                            observation_id: None,
                            existing_id: Some(existing_id),
                            ticket: Some(ImportTicket {
                                id: uuid::Uuid::now_v7().to_string(),
                                reason: reason.clone(),
                            }),
                            error_code: Some("canonical_collision".to_owned()),
                            failure_class: Some(ImportFailureClass::Quarantine),
                            reason: Some(reason),
                            details: None,
                        });
                    }
                }
            }

            if !request_appended_observations.is_empty() {
                if let Some(session) = bulk_session.clone() {
                    self.record_deferred_bulk_import_append(session.clone(), &mut timer)?;
                    self.materialize_bulk_import_append(
                        &session,
                        &request_appended_observations,
                        &mut timer,
                    )?;
                    materialization_state = ImportMaterializationState::Deferred;
                } else {
                    let classification =
                        classify_non_corpus_delta_with_reason(&request_appended_observations);
                    materialization_state = ImportMaterializationState::Classified(classification);
                    self.trigger_append_consumer();
                }
            }

            report.refresh_summary();
            Ok(report)
        })();
        let timing = timer.finish();
        let (result_name, ingested, duplicates, quarantined) = match &result {
            Ok(report) => ("ok", report.ingested, report.duplicates, report.quarantined),
            Err(_) => ("error", 0, 0, 0),
        };
        ObservationImportTimingLog {
            context,
            source_instance_id: source_instance_id.to_owned(),
            timing,
            materialization_state,
            bulk_session_requested,
            result: result_name,
            ingested,
            duplicates,
            quarantined,
        }
        .emit();
        result
    }

    /// Ingest using the v2 wire contract. The legacy method above deliberately
    /// keeps its historical request-level failure semantics for the frozen v1
    /// endpoint.
    pub fn ingest_observation_drafts_v2(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
    ) -> Result<ImportReport, SelfHostError> {
        self.ingest_observation_drafts_v2_with_admission_generation(
            drafts,
            source_instance_id,
            None,
            None,
        )
    }

    pub fn ingest_observation_drafts_v2_with_session(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: Option<&str>,
    ) -> Result<ImportReport, SelfHostError> {
        self.ingest_observation_drafts_v2_with_admission_generation(
            drafts,
            source_instance_id,
            bulk_session_id,
            None,
        )
    }

    pub fn ingest_observation_drafts_v2_with_admission_generation(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: Option<&str>,
        admission_generation: Option<u64>,
    ) -> Result<ImportReport, SelfHostError> {
        self.ingest_observation_drafts_v2_with_admission_generation_and_spawn_wait(
            drafts,
            source_instance_id,
            bulk_session_id,
            admission_generation,
            Duration::ZERO,
        )
    }

    pub(super) fn ingest_observation_drafts_v2_with_admission_generation_and_spawn_wait(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: Option<&str>,
        admission_generation: Option<u64>,
        spawn_blocking_wait: Duration,
    ) -> Result<ImportReport, SelfHostError> {
        let context = ObservationImportContext::from_drafts(&drafts);
        let bulk_session_requested = bulk_session_id.is_some();
        let mut timer = ObservationImportTimer::new();
        timer.record_stage(ImportTimingStage::SpawnBlockingWait, spawn_blocking_wait);
        let mut materialization_state = ImportMaterializationState::NotRun;
        let result = (|| {
            let _import_permit = self.try_acquire_import_permit()?;
            if source_instance_id.trim().is_empty() {
                return Err(SelfHostError::IngestionRequest {
                    code: "source_instance_required",
                    detail: "source_instance_id must not be blank".to_owned(),
                    details: serde_json::json!({"field": "source_instance_id"}),
                });
            }
            self.enforce_cutover_admission_for_import(
                source_instance_id,
                CutoverApiVersion::V2,
                admission_generation,
                &mut timer,
            )?;
            let request_draft_count = drafts.len();

            let _operation = self.bulk_import_operation_lock_for_import(&mut timer)?;
            let core = self.core_snapshot();
            let bulk_session = self.bulk_import_session_for_append(bulk_session_id, &mut timer)?;
            let mut report = ImportReport {
                ingested: 0,
                duplicates: 0,
                quarantined: 0,
                rejected: 0,
                results: Vec::new(),
                summary: ImportSummary::default(),
            };
            let mut item_results: Vec<Option<ImportItemResult>> =
                (0..drafts.len()).map(|_| None).collect();
            let mut prepared = Vec::new();

            for (index, draft) in drafts.into_iter().enumerate() {
                let client_ref = draft
                    .client_ref
                    .clone()
                    .unwrap_or_else(|| index.to_string());
                if index >= self.config.resource_limits.max_import_drafts {
                    item_results[index] = Some(rejected_item(
                        client_ref,
                        "draft_count_exceeded",
                        format!(
                            "draft index {index} exceeds configured maximum {}",
                            self.config.resource_limits.max_import_drafts
                        ),
                        Some(serde_json::json!({
                            "field": "drafts",
                            "actual": request_draft_count,
                            "maximum": self.config.resource_limits.max_import_drafts,
                        })),
                    ));
                    continue;
                }
                if client_ref.trim().is_empty() {
                    item_results[index] = Some(rejected_item(
                        client_ref,
                        "client_ref_required",
                        "client_ref must not be blank".to_owned(),
                        None,
                    ));
                    continue;
                }

                let payload_bytes = serde_json::to_vec(&draft.payload)?.len();
                if payload_bytes > self.config.resource_limits.max_payload_bytes {
                    item_results[index] = Some(rejected_item(
                        client_ref,
                        "payload_too_large",
                        format!(
                            "payload size {payload_bytes} exceeds configured maximum {}",
                            self.config.resource_limits.max_payload_bytes
                        ),
                        Some(serde_json::json!({
                            "field": "payload",
                            "actual_bytes": payload_bytes,
                            "max_bytes": self.config.resource_limits.max_payload_bytes,
                        })),
                    ));
                    continue;
                }

                let draft = match derive_v2_identity(draft, source_instance_id) {
                    Ok(draft) => draft,
                    Err(error) => {
                        item_results[index] = Some(rejected_item(
                            client_ref,
                            error.code,
                            error.reason,
                            error.details,
                        ));
                        continue;
                    }
                };
                match prepare_draft(&core, draft) {
                    Ok(observation) => {
                        if let Some(schema) = core
                            .registry
                            .get_schema_at_version(&observation.schema, &observation.schema_version)
                        {
                            timer.record_surplus_payload_fields(count_surplus_payload_fields(
                                &schema.payload_schema,
                                &observation.payload,
                            ));
                        }
                        prepared.push(PreparedImportObservation {
                            index,
                            client_ref,
                            observation,
                        })
                    }
                    Err(result) => {
                        item_results[index] =
                            Some(item_result_from_ingest_result(client_ref, result));
                    }
                }
            }

            let append_input = prepared
                .iter()
                .map(|item| item.observation.clone())
                .collect::<Vec<_>>();
            let audit_events = if append_input.is_empty() {
                Vec::new()
            } else {
                let event = self.build_audit_event(
                    "actor:self-host",
                    AuditEventKind::WriteExecution,
                    serde_json::json!({
                        "mode": "v2_bulk_observation_import",
                        "source_instance_id": source_instance_id,
                        "requested": append_input.len(),
                        "bulk_session_id": bulk_session_id,
                        "privacy_decisions": privacy_audit_details(&append_input),
                    }),
                )?;
                vec![AppService::audit_record(&event)?]
            };
            let outcomes = if append_input.is_empty() {
                Vec::new()
            } else {
                let persistence = self.persistence_lock_for_import(&mut timer)?;
                let stage_started_at = Instant::now();
                let append_result = persistence.append_observations_v2_with_bridge(
                    source_instance_id,
                    admission_generation,
                    &append_input,
                    &audit_events,
                );
                timer.record_stage(ImportTimingStage::LedgerAppend, stage_started_at.elapsed());
                match append_result {
                    Ok(outcomes) => outcomes,
                    Err(error) => {
                        let reason = format!("durable append temporarily failed: {error}");
                        for item in &prepared {
                            item_results[item.index] = Some(transient_item(
                                item.client_ref.clone(),
                                reason.clone(),
                                serde_json::json!({"stage": "durable_append"}),
                            ));
                        }
                        Vec::new()
                    }
                }
            };
            if !outcomes.is_empty() && outcomes.len() != prepared.len() {
                return Err(SelfHostError::Ingestion(
                    "v2 bulk append returned an unexpected outcome count".to_owned(),
                ));
            }

            let mut request_appended_observations = Vec::new();
            for (item, outcome) in prepared.into_iter().zip(outcomes) {
                let result = match outcome {
                    DurableAppendOutcome::Appended(id) => {
                        if id != item.observation.id {
                            return Err(SelfHostError::Ingestion(
                                "v2 append returned a mismatched observation id".to_owned(),
                            ));
                        }
                        report.ingested += 1;
                        request_appended_observations.push(item.observation);
                        ImportItemResult {
                            client_ref: item.client_ref,
                            outcome: ImportOutcome::Ingested,
                            observation_id: Some(id),
                            existing_id: None,
                            ticket: None,
                            error_code: None,
                            failure_class: None,
                            reason: None,
                            details: None,
                        }
                    }
                    DurableAppendOutcome::Duplicate(existing_id) => {
                        report.duplicates += 1;
                        ImportItemResult {
                            client_ref: item.client_ref,
                            outcome: ImportOutcome::Duplicate,
                            observation_id: None,
                            existing_id: Some(existing_id),
                            ticket: None,
                            error_code: Some("duplicate.existing_id".to_owned()),
                            failure_class: None,
                            reason: None,
                            details: None,
                        }
                    }
                    DurableAppendOutcome::CanonicalCollision(existing_id) => {
                        report.quarantined += 1;
                        let ticket = ImportTicket {
                            id: uuid::Uuid::now_v7().to_string(),
                            reason: format!(
                                "canonical identity collision with existing observation {existing_id}"
                            ),
                        };
                        ImportItemResult {
                            client_ref: item.client_ref,
                            outcome: ImportOutcome::Quarantined,
                            observation_id: None,
                            existing_id: Some(existing_id),
                            ticket: Some(ticket.clone()),
                            error_code: Some("canonical_collision".to_owned()),
                            failure_class: Some(ImportFailureClass::Quarantine),
                            reason: Some(ticket.reason),
                            details: None,
                        }
                    }
                };
                item_results[item.index] = Some(result);
            }

            report.results = item_results
                .into_iter()
                .map(|result| result.expect("v2 import must produce one result per draft"))
                .collect();
            report.quarantined = report
                .results
                .iter()
                .filter(|result| result.outcome == ImportOutcome::Quarantined)
                .count();
            report.rejected = report
                .results
                .iter()
                .filter(|result| result.outcome == ImportOutcome::Rejected)
                .count();

            if !request_appended_observations.is_empty() {
                if let Some(session) = bulk_session {
                    if let Err(error) =
                        self.record_deferred_bulk_import_append(session.clone(), &mut timer)
                    {
                        tracing::error!(
                            error = %error,
                            "v2 import deferred-materialization bookkeeping failed after durable append"
                        );
                    }
                    self.materialize_bulk_import_append(
                        &session,
                        &request_appended_observations,
                        &mut timer,
                    )?;
                    materialization_state = ImportMaterializationState::Deferred;
                } else {
                    let classification =
                        classify_non_corpus_delta_with_reason(&request_appended_observations);
                    materialization_state = ImportMaterializationState::Classified(classification);
                    self.trigger_append_consumer();
                }
            }

            report.refresh_summary();
            Ok(report)
        })();
        let timing = timer.finish();
        let (result_name, ingested, duplicates, quarantined) = match &result {
            Ok(report) => ("ok", report.ingested, report.duplicates, report.quarantined),
            Err(_) => ("error", 0, 0, 0),
        };
        ObservationImportTimingLog {
            context,
            source_instance_id: source_instance_id.to_owned(),
            timing,
            materialization_state,
            bulk_session_requested,
            result: result_name,
            ingested,
            duplicates,
            quarantined,
        }
        .emit();
        result
    }

    // v1 deliberately retains request-level abort semantics. v2 prepares each
    // draft independently in ingest_observation_drafts_v2_with_session.
    fn prepare_legacy_observation_draft_batch(
        &self,
        core: &AppCore,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        timer: &mut ObservationImportTimer,
        prepared_observations: &mut Vec<Observation>,
    ) -> Result<(), SelfHostError> {
        let mut observations = Vec::with_capacity(drafts.len());
        for draft in drafts {
            let payload_bytes = serde_json::to_vec(&draft.payload)?.len();
            if payload_bytes > self.config.resource_limits.max_payload_bytes {
                return Err(SelfHostError::Ingestion(format!(
                    "payload size {payload_bytes} exceeds configured maximum {}",
                    self.config.resource_limits.max_payload_bytes
                )));
            }
            match prepare_draft(core, namespace_draft(draft, source_instance_id)) {
                Ok(observation) => {
                    if let Some(schema) = core
                        .registry
                        .get_schema_at_version(&observation.schema, &observation.schema_version)
                    {
                        timer.record_surplus_payload_fields(count_surplus_payload_fields(
                            &schema.payload_schema,
                            &observation.payload,
                        ));
                    }
                    observations.push(observation)
                }
                Err(IngestResult::Rejected { message, .. }) => {
                    return Err(SelfHostError::Ingestion(message));
                }
                Err(IngestResult::Quarantined { ticket }) => {
                    return Err(SelfHostError::Ingestion(ticket.reason));
                }
                Err(other) => {
                    return Err(SelfHostError::Ingestion(format!(
                        "observation preparer returned an invalid terminal result: {other:?}"
                    )));
                }
            }
        }

        prepared_observations.extend(observations);
        Ok(())
    }
}

fn freshness_thresholds(config: &SelfHostConfig) -> Vec<FreshnessThreshold> {
    let mut thresholds = config
        .freshness
        .threshold_seconds
        .iter()
        .map(|(source_id, seconds)| {
            (
                source_id.clone(),
                FreshnessThreshold {
                    source_id: source_id.clone(),
                    max_age_seconds: *seconds,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    for channel in config.channels.iter().filter(|channel| channel.enabled) {
        thresholds
            .entry(channel.id.clone())
            .or_insert_with(|| FreshnessThreshold {
                source_id: channel.id.clone(),
                max_age_seconds: channel.freshness_threshold_seconds as i64,
            });
    }
    let mut values = thresholds.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| left.source_id.cmp(&right.source_id));
    values
}

mod bulk_import;
mod media_support;
pub(crate) mod projection_api;
mod search_index;
mod service_support;
mod slide_support;
mod supplemental_write;
mod sync;
mod sync_support;

pub use bulk_import::{BulkImportSessionPhase, BulkImportSessionReport};
use media_support::*;
pub use projection_api::CorpusSourceTypeSummary;
#[cfg(test)]
use service_support::classify_slack_ingress;
use service_support::{
    build_channel_registry_projection_lineage, build_mixed_projection_lineage,
    build_person_page_lineage, build_projection_lineage, build_supplemental_projection_lineage,
    consent_status_for_person_id, namespace_draft,
};
use slide_support::*;
pub use supplemental_write::{SupplementalWriteRequest, WriteEnvelope};
use sync_support::*;

#[cfg(test)]
mod tests;
