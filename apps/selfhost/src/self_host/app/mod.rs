use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::attribute_inventory::{AttributeInventoryDocument, build_inventory_documents};
use crate::self_host::config::{GoogleConfig, SelfHostConfig, SlackConfig};
use crate::self_host::google::HttpGoogleSlidesClient;
use crate::self_host::registry::{seed_projection_catalog, seed_registry};
use crate::self_host::slack::HttpSlackClient;
use lethe_adapter_api::config::{
    AdapterConfig, BackoffStrategy, RateLimitConfig, RetryConfig, SchemaBinding,
};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::retry::ResilientExecutor;
use lethe_adapter_api::traits::{ObservationDraft, SourceAdapter};
use lethe_adapter_gslides::gslides::client::GoogleSlidesClient;
use lethe_adapter_gslides::gslides::mapper::GoogleSlidesAdapter;
use lethe_adapter_slack::slack::client::SlackClient;
use lethe_adapter_slack::slack::mapper::SlackAdapter;
use lethe_api::api::envelope::{ProjectionMetadata, ResponseEnvelope};
use lethe_api::api::grep::GrepRecord;
use lethe_api::api::health::{DependencyHealthInfo, HealthResponse, LastSyncHealth, SyncMetrics};
use lethe_api::api::pagination::{PaginatedResponse, PaginationParams, paginate};
use lethe_api::api::read_mode::{ReadModeError, ReadModeResolver};
use lethe_core::domain::{
    ActorRef, AuthorityModel, BlobRef, CaptureModel, EntityRef, IngestResult, Observation,
    ObservationId, ObserverRef, ProjectionHealth, ProjectionRef, ProjectionStatus, ReadMode,
    SchemaRef, SemVer, SourceSystemRef,
};
use lethe_derivation_gemini::{GeminiSlideAnalyzer, SlideAnalysisProjector};
use lethe_engine::identity::projector::IdentityProjector;
use lethe_engine::identity::types::{
    IdentifierType, IdentityResolutionOutput, PersonCandidate, SourceIdentifier,
};
#[cfg(test)]
use lethe_engine::lake::LakeStore;
use lethe_engine::lake::{BlobStore, IngestRequest, ObservationPreparer};
use lethe_engine::projection::catalog::ProjectionCatalog;
use lethe_engine::projection::lineage::{LineageManifest, SourceSnapshot};
use lethe_engine::supplemental::SupplementalStore;
use lethe_policy::governance::audit::{AuditLog, InMemoryAuditLog};
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
    CardQueueProjection, CardQueueProjector, CognitionStateProjector, FreshnessProjection,
    FreshnessProjector, FreshnessStatus, FreshnessThreshold, PlanStateProjection, ReplyLatency,
    ReplySloProjection, ReplySloProjector, ReplySloStatus, ResumeSnapshotProjection,
    SourceFreshness,
};
use lethe_projection_corpus::CorpusProjector;
use lethe_projection_person::person_page::projector::PersonPageProjector;
use lethe_projection_person::person_page::types::{
    FrontendProfile, IdentityInfo, PersonActivity, PersonDetailResponse, PersonListItem,
    PersonMessage, PersonPageOutput, PersonProfile, PersonSlide, TimelineEvent,
};
use lethe_storage_api::{
    AppendOutcome as DurableAppendOutcome, ObservationStats, ProjectionItem, ProjectionItemCommit,
    StorageError, StoragePorts, StoredObservation,
};
use lethe_storage_sqlite::persistence::{PersistenceError, SqlitePersistence};

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
    #[error("read mode error: {0}")]
    ReadMode(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("policy denied: {0}")]
    Policy(String),
    #[error("authentication failed: {0}")]
    Auth(String),
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

const NON_CORPUS_MATERIALIZATION_VERSION: u32 = 6;
const REPLY_SLO_ITEM_OWNER: &str = "__reply_slo__";
const NON_CORPUS_REBUILD_STAGING_PROJECTION_ID: &str = "proj:person-page:rebuild-staging";
const CANONICAL_OBSERVATION_FINGERPRINT_DOMAIN: &[u8] =
    b"lethe:canonical-observation-fingerprint:v1\0";
const IMPORT_PROCESS_BATCH_SIZE: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NonCorpusDeltaKind {
    FreshnessOnly,
    SlackMessage,
    FullRebuild,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactProjectionState {
    next_identity_ordinal: u64,
    identity_candidates: Vec<CompactIdentityCandidate>,
    consent_decisions: Vec<CompactConsentDecision>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactIdentityCandidate {
    first_ordinal: u64,
    slack_user_id: Option<String>,
    observation_ids: Vec<String>,
    candidate: PersonCandidate,
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

#[derive(Debug, Clone)]
struct PendingProjectionItemCommit {
    base_person_message_count: u64,
    base_reply_slo_count: u64,
    replaced_person_owners: BTreeSet<String>,
    deleted_person_item_owners: BTreeMap<String, String>,
    commit: ProjectionItemCommit,
}

struct CompactIdentityResolution {
    identity: IdentityResolutionOutput,
    members_by_person: BTreeMap<String, BTreeSet<u64>>,
}

trait ComponentProjectionLookup {
    fn stored_observation(
        &self,
        observation_id: &ObservationId,
    ) -> Result<Option<StoredObservation>, SelfHostError>;

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

    fn person_message_items(&self, owner_key: &str) -> Result<Vec<ProjectionItem>, SelfHostError> {
        Ok(self
            .storage
            .projection_items_by_owner(&ProjectionRef::new("proj:person-page"), owner_key)?)
    }
}

struct SupplementalMaterializedDelta {
    materialized: MaterializedProjectionSnapshot,
    item_commit: ProjectionItemCommit,
}

type FrontendProfileSelections = BTreeMap<String, (usize, DateTime<Utc>, FrontendProfile)>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MaterializedProjectionSnapshot {
    format_version: u32,
    last_append_seq: u64,
    observation_count: u64,
    canonical_observation_fingerprint: String,
    supplemental_fingerprint: String,
    compact_state: CompactProjectionState,
    person_consents: BTreeMap<String, ConsentStatus>,
    person_message_count: u64,
    reply_slo_count: u64,
    snapshot: ProjectionSnapshot,
    #[serde(skip)]
    pending_item_commit: Option<PendingProjectionItemCommit>,
}

impl MaterializedProjectionSnapshot {
    fn observation_stats(&self) -> ObservationStats {
        ObservationStats {
            count: self.observation_count,
            max_append_seq: self.last_append_seq,
        }
    }

    fn matches(&self, stats: ObservationStats, supplemental_fingerprint: &str) -> bool {
        self.format_version == NON_CORPUS_MATERIALIZATION_VERSION
            && self.last_append_seq == stats.max_append_seq
            && self.observation_count == stats.count
            && self.supplemental_fingerprint == supplemental_fingerprint
    }

    fn validate(&self) -> Result<(), SelfHostError> {
        decode_canonical_observation_fingerprint(&self.canonical_observation_fingerprint)?;
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
        if let Some(pending) = &self.pending_item_commit {
            validate_pending_projection_item_commit(
                pending,
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

#[derive(Debug)]
pub struct AppCore {
    pub registry: lethe_registry::registry::RegistryStore,
    pub catalog: ProjectionCatalog,
    pub blobs: BlobStore,
    pub supplemental: SupplementalStore,
    freshness_thresholds: Vec<FreshnessThreshold>,
    observation_stats: ObservationStats,
    canonical_observation_fingerprint: String,
    supplemental_fingerprint: String,
    compact_state: CompactProjectionState,
    person_consents: BTreeMap<String, ConsentStatus>,
    person_message_count: u64,
    reply_slo_count: u64,
    pub snapshot: ProjectionSnapshot,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub last_sync_error: Option<String>,
    pub sync_metrics: SyncMetrics,
}

impl AppCore {
    fn from_materialized(
        materialized: MaterializedProjectionSnapshot,
        persisted_blobs: Vec<Vec<u8>>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
        freshness_thresholds: Vec<FreshnessThreshold>,
        channels: Vec<lethe_registry::registry::ChannelRecord>,
    ) -> Result<Self, SelfHostError> {
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

        materialized.validate()?;
        let mut core = Self {
            registry,
            catalog: seed_projection_catalog(),
            blobs,
            supplemental,
            freshness_thresholds,
            observation_stats: materialized.observation_stats(),
            canonical_observation_fingerprint: materialized.canonical_observation_fingerprint,
            supplemental_fingerprint: materialized.supplemental_fingerprint,
            compact_state: materialized.compact_state,
            person_consents: materialized.person_consents,
            person_message_count: materialized.person_message_count,
            reply_slo_count: materialized.reply_slo_count,
            snapshot: materialized.snapshot,
            last_sync_at: None,
            last_sync_error: None,
            sync_metrics: SyncMetrics::default(),
        };
        core.activate_projections();
        Ok(core)
    }

    fn install_materialized(&mut self, materialized: MaterializedProjectionSnapshot) {
        self.observation_stats = materialized.observation_stats();
        self.canonical_observation_fingerprint = materialized.canonical_observation_fingerprint;
        self.supplemental_fingerprint = materialized.supplemental_fingerprint;
        self.compact_state = materialized.compact_state;
        self.person_consents = materialized.person_consents;
        self.person_message_count = materialized.person_message_count;
        self.reply_slo_count = materialized.reply_slo_count;
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
    ) -> Result<lethe_engine::supplemental::store::UpsertRollback, lethe_core::domain::DomainError>
    where
        ObservationExists: Fn(&lethe_core::domain::ObservationId) -> bool,
        SupplementalExists: Fn(&lethe_core::domain::SupplementalId) -> bool,
    {
        self.supplemental.upsert_with_rollback_checked(
            record,
            observation_exists,
            supplemental_exists,
        )
    }

    fn rollback_supplemental(
        &mut self,
        rollback: lethe_engine::supplemental::store::UpsertRollback,
    ) {
        self.supplemental.rollback_upsert(rollback);
    }
}

fn prepare_draft(core: &AppCore, draft: ObservationDraft) -> Result<Observation, IngestResult> {
    ObservationPreparer::new(&core.registry, &core.blobs).prepare(IngestRequest {
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

#[derive(Clone)]
pub struct AppService {
    core: Arc<Mutex<AppCore>>,
    persistence: Arc<Mutex<Box<dyn StoragePorts>>>,
    search_index: search_index::SearchIndexManager,
    config: Arc<SelfHostConfig>,
    slack_sources: Vec<SlackSourceRuntime>,
    google_sources: Vec<GoogleSourceRuntime>,
    slide_analyzer: Option<GeminiSlideAnalyzer>,
    resilient_executor: Arc<ResilientExecutor>,
    audit_log: Arc<InMemoryAuditLog>,
    #[cfg(test)]
    non_corpus_rebuild_count: Arc<std::sync::atomic::AtomicUsize>,
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
        let resume_snapshot = cognition_projector.resume_snapshot(&all_supplemental_records);
        let plan_state = cognition_projector.plan_state(&all_supplemental_records);
        let card_queue =
            CardQueueProjector::new(built_at).project_records(&all_supplemental_records);
        let reply_slo = ReplySloProjector::new(built_at)
            .project_records(lake.list(), &all_supplemental_records);
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
}

impl CompactProjectionState {
    fn build(observations: &[Observation]) -> Result<Self, SelfHostError> {
        let mut state = Self {
            next_identity_ordinal: 0,
            identity_candidates: Vec::new(),
            consent_decisions: Vec::new(),
        };
        let mut slack_candidate_index = BTreeMap::new();
        for observation in observations {
            state.capture_consent_decision(observation);
            for candidate in
                IdentityProjector::extract_candidates(std::slice::from_ref(observation))
            {
                state.add_identity_candidate(
                    candidate,
                    observation.id.as_str(),
                    &mut slack_candidate_index,
                )?;
            }
        }
        state.validate()?;
        Ok(state)
    }

    fn with_observation_delta(
        &self,
        observations: &[Observation],
    ) -> Result<(Self, BTreeSet<u64>), SelfHostError> {
        let mut state = self.clone();
        let touched_members = state.apply_observation_page(observations)?;
        Ok((state, touched_members))
    }

    fn apply_observation_page(
        &mut self,
        observations: &[Observation],
    ) -> Result<BTreeSet<u64>, SelfHostError> {
        let mut slack_candidate_index = self
            .identity_candidates
            .iter()
            .enumerate()
            .filter_map(|(index, candidate)| {
                candidate
                    .slack_user_id
                    .as_ref()
                    .map(|user_id| (user_id.clone(), index))
            })
            .collect::<BTreeMap<_, _>>();
        let mut touched_members = BTreeSet::new();
        for observation in observations {
            self.capture_consent_decision(observation);
            let candidates =
                IdentityProjector::extract_candidates(std::slice::from_ref(observation));
            if observation.schema.as_str() == "schema:slack-message" && candidates.len() != 1 {
                return Err(SelfHostError::Ingestion(format!(
                    "Slack observation {} must yield exactly one identity candidate for compact incremental materialization",
                    observation.id
                )));
            }
            for candidate in candidates {
                touched_members.insert(self.add_identity_candidate(
                    candidate,
                    observation.id.as_str(),
                    &mut slack_candidate_index,
                )?);
            }
        }
        self.validate()?;
        Ok(touched_members)
    }

    fn add_identity_candidate(
        &mut self,
        mut candidate: PersonCandidate,
        observation_id: &str,
        slack_candidate_index: &mut BTreeMap<String, usize>,
    ) -> Result<u64, SelfHostError> {
        let ordinal = self.next_identity_ordinal;
        self.next_identity_ordinal =
            self.next_identity_ordinal.checked_add(1).ok_or_else(|| {
                SelfHostError::Ingestion("identity candidate ordinal overflow".to_owned())
            })?;
        sort_identity_identifiers(&mut candidate.identifiers);

        let slack_user_id = slack_user_id_for_candidate(&candidate);
        if candidate.source == "slack" {
            let slack_user_id = slack_user_id.ok_or_else(|| {
                SelfHostError::Ingestion(
                    "Slack identity candidate is missing its source user_id".to_owned(),
                )
            })?;
            if let Some(existing_index) = slack_candidate_index.get(&slack_user_id).copied() {
                let existing = self
                    .identity_candidates
                    .get_mut(existing_index)
                    .ok_or_else(|| {
                        SelfHostError::Ingestion(format!(
                            "compact Slack candidate index is invalid for user {slack_user_id}"
                        ))
                    })?;
                merge_source_internal_candidate(&mut existing.candidate, candidate);
                if existing
                    .observation_ids
                    .iter()
                    .any(|existing_id| existing_id == observation_id)
                {
                    return Err(SelfHostError::Ingestion(format!(
                        "identity candidate {} repeats observation {observation_id}",
                        existing.first_ordinal
                    )));
                }
                existing.observation_ids.push(observation_id.to_owned());
                return Ok(existing.first_ordinal);
            }
            let candidate_index = self.identity_candidates.len();
            self.identity_candidates.push(CompactIdentityCandidate {
                first_ordinal: ordinal,
                slack_user_id: Some(slack_user_id.clone()),
                observation_ids: vec![observation_id.to_owned()],
                candidate,
            });
            slack_candidate_index.insert(slack_user_id, candidate_index);
        } else {
            self.identity_candidates.push(CompactIdentityCandidate {
                first_ordinal: ordinal,
                slack_user_id: None,
                observation_ids: vec![observation_id.to_owned()],
                candidate,
            });
        }
        Ok(ordinal)
    }

    fn capture_consent_decision(&mut self, observation: &Observation) {
        if observation.schema.as_str()
            != lethe_projection_person::person_page::projector::CONSENT_DECISION_SCHEMA
        {
            return;
        }
        self.consent_decisions.push(CompactConsentDecision {
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
        });
    }

    fn resolve_identity(&self) -> IdentityResolutionOutput {
        self.resolve_identity_with_components().identity
    }

    fn resolve_identity_with_components(&self) -> CompactIdentityResolution {
        let mut entries = self.identity_candidates.iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.first_ordinal);
        let candidates = entries
            .iter()
            .map(|entry| entry.candidate.clone())
            .collect::<Vec<_>>();
        let matches = IdentityProjector::cross_source_match(&candidates);
        let (identity, grouped_members) =
            IdentityProjector::new("1.0.0").resolve_with_component_members(&candidates, &matches);
        let members_by_person = identity
            .resolved_persons
            .iter()
            .zip(grouped_members)
            .map(|(person, members)| {
                let members = members
                    .into_iter()
                    .map(|member_index| entries[member_index].first_ordinal)
                    .collect();
                (person.person_id.as_str().to_owned(), members)
            })
            .collect();
        CompactIdentityResolution {
            identity,
            members_by_person,
        }
    }

    fn person_consents(
        &self,
        identity: &IdentityResolutionOutput,
    ) -> BTreeMap<String, ConsentStatus> {
        identity
            .resolved_persons
            .iter()
            .map(|person| {
                let decision = self
                    .consent_decisions
                    .iter()
                    .filter(|decision| {
                        decision.subject == person.person_id.as_str()
                            || decision.identifier.as_ref().is_some_and(|identifier| {
                                person
                                    .identifiers
                                    .iter()
                                    .any(|candidate| candidate.value == *identifier)
                            })
                    })
                    .max_by(|left, right| {
                        (
                            left.published,
                            left.recorded_at,
                            left.observation_id.as_str(),
                        )
                            .cmp(&(
                                right.published,
                                right.recorded_at,
                                right.observation_id.as_str(),
                            ))
                    });
                let status = decision
                    .and_then(|decision| decision.status.as_deref())
                    .and_then(compact_consent_status)
                    .unwrap_or_default();
                (person.person_id.as_str().to_owned(), status)
            })
            .collect()
    }

    fn validate(&self) -> Result<(), SelfHostError> {
        let mut previous_ordinal = None;
        let mut slack_user_ids = BTreeSet::new();
        for entry in &self.identity_candidates {
            if entry.first_ordinal >= self.next_identity_ordinal {
                return Err(SelfHostError::Ingestion(format!(
                    "identity candidate ordinal {} is outside next ordinal {}",
                    entry.first_ordinal, self.next_identity_ordinal
                )));
            }
            if previous_ordinal.is_some_and(|previous| previous >= entry.first_ordinal) {
                return Err(SelfHostError::Ingestion(
                    "compact identity candidates are not in first-observation order".to_owned(),
                ));
            }
            previous_ordinal = Some(entry.first_ordinal);
            if entry.observation_ids.is_empty() {
                return Err(SelfHostError::Ingestion(format!(
                    "identity candidate {} has no source observation",
                    entry.first_ordinal
                )));
            }
            let mut observation_ids = BTreeSet::new();
            for observation_id in &entry.observation_ids {
                if observation_id.trim().is_empty() {
                    return Err(SelfHostError::Ingestion(format!(
                        "identity candidate {} has a blank source observation",
                        entry.first_ordinal
                    )));
                }
                if !observation_ids.insert(observation_id) {
                    return Err(SelfHostError::Ingestion(format!(
                        "identity candidate {} repeats source observation {observation_id}",
                        entry.first_ordinal
                    )));
                }
            }
            match (&entry.slack_user_id, entry.candidate.source.as_str()) {
                (Some(user_id), "slack") => {
                    if slack_user_id_for_candidate(&entry.candidate).as_deref()
                        != Some(user_id.as_str())
                    {
                        return Err(SelfHostError::Ingestion(format!(
                            "compact Slack identity candidate does not match user {user_id}"
                        )));
                    }
                    if !slack_user_ids.insert(user_id.clone()) {
                        return Err(SelfHostError::Ingestion(format!(
                            "compact identity state contains duplicate Slack user {user_id}"
                        )));
                    }
                }
                (None, source) if source != "slack" => {}
                _ => {
                    return Err(SelfHostError::Ingestion(
                        "compact identity candidate source/key invariant failed".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn compact_consent_status(value: &str) -> Option<ConsentStatus> {
    match value {
        "unrestricted" => Some(ConsentStatus::Unrestricted),
        "restricted_capture" => Some(ConsentStatus::RestrictedCapture),
        "opted_out" => Some(ConsentStatus::OptedOut),
        _ => None,
    }
}

fn slack_user_id_for_candidate(candidate: &PersonCandidate) -> Option<String> {
    candidate
        .identifiers
        .iter()
        .find(|identifier| {
            identifier.source == "slack" && identifier.identifier_type == IdentifierType::UserId
        })
        .map(|identifier| identifier.value.clone())
}

fn merge_source_internal_candidate(target: &mut PersonCandidate, incoming: PersonCandidate) {
    target.observed_at = target.observed_at.max(incoming.observed_at);
    if target.display_name.is_none() {
        target.display_name = incoming.display_name;
    }
    for identifier in incoming.identifiers {
        if !target.identifiers.contains(&identifier) {
            target.identifiers.push(identifier);
        }
    }
    sort_identity_identifiers(&mut target.identifiers);
}

fn sort_identity_identifiers(identifiers: &mut Vec<SourceIdentifier>) {
    identifiers.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then(
                identity_identifier_rank(left.identifier_type)
                    .cmp(&identity_identifier_rank(right.identifier_type)),
            )
            .then(left.value.cmp(&right.value))
    });
    identifiers.dedup();
}

fn identity_identifier_rank(identifier_type: IdentifierType) -> u8 {
    match identifier_type {
        IdentifierType::Email => 0,
        IdentifierType::SlackId => 1,
        IdentifierType::ExternalId => 2,
        IdentifierType::ArbitraryKey => 3,
        IdentifierType::UserId => 4,
        IdentifierType::DisplayName => 5,
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
        let mut built = ProjectionSnapshot::build_with_state(
            observations,
            persisted_supplementals,
            freshness_thresholds,
            channels,
            stats,
            built_at,
        )?;
        normalize_person_fact_ids(&mut built.snapshot.person_page, &fact_append_sequences)?;
        let (projection_item_commit, person_message_count, reply_slo_count) =
            detach_projection_items(&mut built.snapshot)?;
        let materialized = Self {
            format_version: NON_CORPUS_MATERIALIZATION_VERSION,
            last_append_seq: stats.max_append_seq,
            observation_count: stats.count,
            canonical_observation_fingerprint: built.canonical_observation_fingerprint,
            supplemental_fingerprint: built.supplemental_fingerprint,
            compact_state: built.compact_state,
            person_consents: built.person_consents,
            person_message_count,
            reply_slo_count,
            snapshot: built.snapshot,
            pending_item_commit: Some(PendingProjectionItemCommit {
                base_person_message_count: 0,
                base_reply_slo_count: 0,
                replaced_person_owners: BTreeSet::new(),
                deleted_person_item_owners: BTreeMap::new(),
                commit: projection_item_commit,
            }),
        };
        materialized.validate()?;
        Ok(materialized)
    }

    fn compact_incremental_delta(
        core: &AppCore,
        appended_observations: &[Observation],
        stats: ObservationStats,
        built_at: DateTime<Utc>,
        lookup: &dyn ComponentProjectionLookup,
    ) -> Result<Box<MaterializedProjectionSnapshot>, SelfHostError> {
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

        let supplemental_records = core
            .supplemental
            .list()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let current_supplemental_fingerprint = supplemental_fingerprint(&supplemental_records)?;
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
        let (compact_state, touched_members) = core
            .compact_state
            .with_observation_delta(appended_observations)?;
        let current_resolution = core.compact_state.resolve_identity_with_components();
        let next_resolution = compact_state.resolve_identity_with_components();
        let identity = next_resolution.identity.clone();
        let person_consents = compact_state.person_consents(&identity);
        let mut snapshot = core.snapshot.clone();
        let mut message_inserts = Vec::new();
        let mut message_updates = Vec::new();
        let mut message_deletes = Vec::new();
        let mut message_delete_owners = BTreeMap::new();
        let mut replaced_person_owners = BTreeSet::new();
        if appended_observations
            .iter()
            .any(|observation| observation.schema.as_str() == "schema:slack-message")
        {
            let appended_fact_sequences = stored_observation_append_sequences(
                lookup,
                appended_observations,
                stats.max_append_seq,
            )?;
            let person_page_delta = match increment_person_page_for_slack(
                &snapshot.identity,
                &identity,
                &snapshot.person_page,
                &core.person_consents,
                &person_consents,
                appended_observations,
                &appended_fact_sequences,
            )? {
                Some(person_page) => person_page,
                None => reproject_affected_person_components(
                    &compact_state,
                    &current_resolution,
                    &next_resolution,
                    &snapshot.person_page,
                    &core.person_consents,
                    &person_consents,
                    &touched_members,
                    &supplemental_records,
                    stats.max_append_seq,
                    lookup,
                )?,
            };
            snapshot.identity = identity;
            snapshot.person_page = person_page_delta.person_page;
            message_inserts = person_page_delta.message_inserts;
            message_updates = person_page_delta.message_updates;
            message_deletes = person_page_delta.message_deletes;
            message_delete_owners = person_page_delta.message_delete_owners;
            replaced_person_owners = person_page_delta.replaced_person_owners;
        }
        let inserted_person_message_count = u64::try_from(message_inserts.len()).map_err(|_| {
            SelfHostError::Ingestion("incremental person message count does not fit u64".to_owned())
        })?;
        let deleted_person_message_count = u64::try_from(message_deletes.len()).map_err(|_| {
            SelfHostError::Ingestion("deleted person message count does not fit u64".to_owned())
        })?;
        let person_message_count = core
            .person_message_count
            .checked_add(inserted_person_message_count)
            .and_then(|count| count.checked_sub(deleted_person_message_count))
            .ok_or_else(|| {
                SelfHostError::Ingestion(
                    "person message count overflow or underflow during incremental materialization"
                        .to_owned(),
                )
            })?;
        snapshot.freshness = freshness_projection_after_delta(
            &snapshot.freshness,
            &core.freshness_thresholds,
            appended_observations,
            built_at,
        )?;

        let cognition_projector = CognitionStateProjector::new(built_at);
        snapshot.resume_snapshot = cognition_projector.resume_snapshot(&supplemental_records);
        snapshot.plan_state = cognition_projector.plan_state(&supplemental_records);
        snapshot.card_queue =
            CardQueueProjector::new(built_at).project_records(&supplemental_records);
        let reply_slo_delta = ReplySloProjector::new(built_at)
            .project_records(appended_observations, &supplemental_records);
        let reply_slo_upserts = reply_slo_delta
            .rows
            .iter()
            .map(reply_slo_projection_item)
            .collect::<Result<Vec<_>, _>>()?;
        let appended_reply_slo_count = u64::try_from(reply_slo_upserts.len()).map_err(|_| {
            SelfHostError::Ingestion("incremental reply SLO count does not fit u64".to_owned())
        })?;
        let reply_slo_count = core
            .reply_slo_count
            .checked_add(appended_reply_slo_count)
            .ok_or_else(|| {
                SelfHostError::Ingestion(
                    "reply SLO count overflow during incremental materialization".to_owned(),
                )
            })?;
        if !snapshot.reply_slo.rows.is_empty() || !snapshot.reply_slo.overdue.is_empty() {
            return Err(SelfHostError::Ingestion(
                "incremental materialization received resident reply SLO rows".to_owned(),
            ));
        }
        snapshot.built_at = built_at;
        snapshot.lineage = build_person_page_lineage(
            &canonical_observation_fingerprint,
            stats,
            &current_supplemental_fingerprint,
            supplemental_records.len(),
            person_page_output_count(&snapshot.person_page, person_message_count)?,
            built_at,
        );

        let materialized = Self {
            format_version: NON_CORPUS_MATERIALIZATION_VERSION,
            last_append_seq: stats.max_append_seq,
            observation_count: stats.count,
            canonical_observation_fingerprint,
            supplemental_fingerprint: current_supplemental_fingerprint,
            compact_state,
            person_consents,
            person_message_count,
            reply_slo_count,
            snapshot,
            pending_item_commit: Some(PendingProjectionItemCommit {
                base_person_message_count: core.person_message_count,
                base_reply_slo_count: core.reply_slo_count,
                replaced_person_owners,
                deleted_person_item_owners: message_delete_owners,
                commit: ProjectionItemCommit::Delta {
                    inserts: message_inserts
                        .into_iter()
                        .chain(reply_slo_upserts)
                        .collect(),
                    updates: message_updates,
                    deletes: message_deletes,
                },
            }),
        };
        materialized.validate()?;
        Ok(Box::new(materialized))
    }
}

fn classify_non_corpus_delta(observations: &[Observation]) -> NonCorpusDeltaKind {
    const FRESHNESS_ONLY_SCHEMAS: &[&str] = &[
        "schema:claude-message",
        "schema:chatgpt-message",
        "schema:github-event",
        "schema:coding-agent-message",
        "schema:gmail-message",
        "schema:discord-message",
    ];

    if observations.is_empty() {
        return NonCorpusDeltaKind::FullRebuild;
    }
    let mut saw_slack_message = false;
    for observation in observations {
        match observation.schema.as_str() {
            "schema:slack-message"
                if observation
                    .payload
                    .get("user_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|user_id| !user_id.trim().is_empty()) =>
            {
                saw_slack_message = true;
            }
            schema
                if FRESHNESS_ONLY_SCHEMAS.contains(&schema)
                    && !contributes_to_reply_slo(observation) => {}
            _ => return NonCorpusDeltaKind::FullRebuild,
        }
    }
    if saw_slack_message {
        NonCorpusDeltaKind::SlackMessage
    } else {
        NonCorpusDeltaKind::FreshnessOnly
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

struct IncrementalPersonPageResult {
    person_page: PersonPageOutput,
    message_inserts: Vec<ProjectionItem>,
    message_updates: Vec<ProjectionItem>,
    message_deletes: Vec<String>,
    message_delete_owners: BTreeMap<String, String>,
    replaced_person_owners: BTreeSet<String>,
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

fn increment_person_page_for_slack(
    current_identity: &IdentityResolutionOutput,
    next_identity: &IdentityResolutionOutput,
    current: &PersonPageOutput,
    current_consents: &BTreeMap<String, ConsentStatus>,
    next_consents: &BTreeMap<String, ConsentStatus>,
    appended_observations: &[Observation],
    fact_append_sequences: &BTreeMap<String, u64>,
) -> Result<Option<IncrementalPersonPageResult>, SelfHostError> {
    if !current.messages.is_empty() {
        return Err(SelfHostError::Ingestion(
            "incremental person-page received resident historical messages".to_owned(),
        ));
    }
    let current_identifier_owners = identity_identifier_owners(current_identity);
    let next_identifier_owners = identity_identifier_owners(next_identity);
    if current_identifier_owners
        .iter()
        .any(|(identifier, owner)| next_identifier_owners.get(identifier) != Some(owner))
    {
        return Ok(None);
    }
    for person in &current_identity.resolved_persons {
        let person_id = person.person_id.as_str();
        if !next_identity
            .resolved_persons
            .iter()
            .any(|candidate| candidate.person_id == person.person_id)
            || current_consents.get(person_id) != next_consents.get(person_id)
        {
            return Ok(None);
        }
    }

    let mut profiles_by_person = current
        .profiles
        .iter()
        .cloned()
        .map(|profile| (profile.person_id.as_str().to_owned(), profile))
        .collect::<BTreeMap<_, _>>();
    let mut slides_by_person = BTreeMap::<String, Vec<_>>::new();
    for slide in &current.slides {
        slides_by_person
            .entry(slide.person_id.as_str().to_owned())
            .or_default()
            .push(slide.clone());
    }
    let mut activities_by_person = BTreeMap::new();
    for activity in &current.activities {
        if activities_by_person
            .insert(activity.person_id.as_str().to_owned(), activity.clone())
            .is_some()
        {
            return Err(SelfHostError::Ingestion(format!(
                "resident person-page contains duplicate activity for {}",
                activity.person_id
            )));
        }
    }

    let mut message_upserts = Vec::new();
    for observation in appended_observations
        .iter()
        .filter(|observation| observation.schema.as_str() == "schema:slack-message")
    {
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
        let person_id = next_identifier_owners
            .get(user_id)
            .cloned()
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "Slack identity node {user_id} has no resolved component"
                ))
            })?;
        {
            if next_consents.get(&person_id) == Some(&ConsentStatus::OptedOut) {
                continue;
            }
            let person_ref = EntityRef::new(&person_id);
            let activity = activities_by_person
                .entry(person_id.clone())
                .or_insert_with(|| PersonActivity {
                    person_id: person_ref.clone(),
                    total_slides_related: 0,
                    total_messages: 0,
                    first_activity: None,
                    last_activity: None,
                    active_channels: Vec::new(),
                });
            activity.total_messages = activity.total_messages.checked_add(1).ok_or_else(|| {
                SelfHostError::Ingestion(format!("person message count overflow for {person_id}"))
            })?;
            let mut message = person_message_from_slack(observation, &person_id);
            let append_seq = fact_append_sequences
                .get(observation.id.as_str())
                .copied()
                .ok_or_else(|| {
                    SelfHostError::Ingestion(format!(
                        "Slack message {} has no append sequence",
                        observation.id
                    ))
                })?;
            message.id = materialized_message_id(append_seq, &observation.id);
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
            message_upserts.push(person_message_projection_item(&message)?);
        }
    }

    let mut profiles = Vec::new();
    let mut slides = Vec::new();
    let mut activities = Vec::new();
    for person in &next_identity.resolved_persons {
        let person_id = person.person_id.as_str();
        if next_consents.get(person_id) == Some(&ConsentStatus::OptedOut) {
            continue;
        }
        let mut person_slides = slides_by_person.remove(person_id).unwrap_or_default();
        for slide in &mut person_slides {
            slide.person_id = person.person_id.clone();
        }
        let mut activity = activities_by_person
            .remove(person_id)
            .unwrap_or_else(|| person_activity(&person.person_id, &person_slides, &[]));
        if activity.total_slides_related != person_slides.len() {
            return Err(SelfHostError::Ingestion(format!(
                "resident activity slide count for {person_id} is {}, expected {}",
                activity.total_slides_related,
                person_slides.len()
            )));
        }
        activity.person_id = person.person_id.clone();
        let profile = match profiles_by_person.remove(person_id) {
            Some(mut profile) => {
                profile.display_name = person.canonical_name.clone();
                profile.identities = person_identity_info(person);
                profile.source_count = person.sources.len();
                profile.last_activity = activity.last_activity;
                if let Some(last_activity) = activity.last_activity {
                    profile.profile_updated_at = profile.profile_updated_at.max(last_activity);
                }
                profile
            }
            None => PersonProfile {
                person_id: person.person_id.clone(),
                display_name: person.canonical_name.clone(),
                self_intro_text: None,
                self_intro_slide_id: None,
                self_intro_thumbnail: None,
                identities: person_identity_info(person),
                source_count: person.sources.len(),
                last_activity: activity.last_activity,
                profile_updated_at: activity
                    .last_activity
                    .or(activity.first_activity)
                    .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
                frontend_profile: None,
            },
        };
        profiles.push(profile);
        slides.extend(person_slides);
        activities.push(activity);
    }

    if !profiles_by_person.is_empty()
        || !slides_by_person.is_empty()
        || !activities_by_person.is_empty()
    {
        return Err(SelfHostError::Ingestion(
            "resident person-page contains records not represented by compact identity state"
                .to_owned(),
        ));
    }
    Ok(Some(IncrementalPersonPageResult {
        person_page: PersonPageOutput {
            profiles,
            slides,
            messages: Vec::new(),
            activities,
        },
        message_inserts: message_upserts,
        message_updates: Vec::new(),
        message_deletes: Vec::new(),
        message_delete_owners: BTreeMap::new(),
        replaced_person_owners: BTreeSet::new(),
    }))
}

#[allow(clippy::too_many_arguments)]
fn reproject_affected_person_components(
    next_state: &CompactProjectionState,
    current_resolution: &CompactIdentityResolution,
    next_resolution: &CompactIdentityResolution,
    current_page: &PersonPageOutput,
    current_consents: &BTreeMap<String, ConsentStatus>,
    next_consents: &BTreeMap<String, ConsentStatus>,
    touched_members: &BTreeSet<u64>,
    supplemental_records: &[lethe_core::domain::SupplementalRecord],
    canonical_high_water: u64,
    lookup: &dyn ComponentProjectionLookup,
) -> Result<IncrementalPersonPageResult, SelfHostError> {
    if !current_page.messages.is_empty() {
        return Err(SelfHostError::Ingestion(
            "component re-projection received resident historical messages".to_owned(),
        ));
    }

    let affected_members = affected_identity_members(
        current_resolution,
        next_resolution,
        current_consents,
        next_consents,
        touched_members,
    );
    if affected_members.is_empty() {
        return Err(SelfHostError::Ingestion(
            "identity delta did not identify an affected component".to_owned(),
        ));
    }

    let affected_current_people =
        component_people_intersecting(&current_resolution.members_by_person, &affected_members);
    let affected_next_people =
        component_people_intersecting(&next_resolution.members_by_person, &affected_members);

    let mut source_observation_ids = BTreeSet::new();
    for entry in &next_state.identity_candidates {
        if affected_members.contains(&entry.first_ordinal) {
            source_observation_ids.extend(entry.observation_ids.iter().cloned());
        }
    }
    if source_observation_ids.is_empty() {
        return Err(SelfHostError::Ingestion(
            "affected identity components have no source observations".to_owned(),
        ));
    }

    let mut observations_by_append_seq = BTreeMap::new();
    for observation_id in &source_observation_ids {
        let observation_ref = ObservationId::new(observation_id);
        let stored = lookup
            .stored_observation(&observation_ref)?
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "affected component references missing observation {observation_id}"
                ))
            })?;
        if stored.observation.id != observation_ref {
            return Err(SelfHostError::Ingestion(format!(
                "affected component lookup returned {} for {observation_id}",
                stored.observation.id
            )));
        }
        if stored.append_seq == 0 || stored.append_seq > canonical_high_water {
            return Err(SelfHostError::Ingestion(format!(
                "affected observation {observation_id} has append sequence {} outside high-water {canonical_high_water}",
                stored.append_seq
            )));
        }
        if let Some(previous) =
            observations_by_append_seq.insert(stored.append_seq, stored.observation)
        {
            return Err(SelfHostError::Ingestion(format!(
                "affected observations {} and {observation_id} share append sequence {}",
                previous.id, stored.append_seq
            )));
        }
    }
    let fact_append_sequences = observations_by_append_seq
        .iter()
        .map(|(append_seq, observation)| (observation.id.as_str().to_owned(), *append_seq))
        .collect::<BTreeMap<_, _>>();
    let observations = observations_by_append_seq.into_values().collect::<Vec<_>>();

    let affected_identity = IdentityResolutionOutput {
        resolved_persons: next_resolution
            .identity
            .resolved_persons
            .iter()
            .filter(|person| affected_next_people.contains(person.person_id.as_str()))
            .cloned()
            .collect(),
        candidates: Vec::new(),
        person_identifiers: next_resolution
            .identity
            .person_identifiers
            .iter()
            .filter(|row| affected_next_people.contains(row.person_id.as_str()))
            .cloned()
            .collect(),
    };
    let affected_supplementals = supplemental_records
        .iter()
        .filter(|record| {
            record
                .derived_from
                .observations
                .iter()
                .any(|observation_id| source_observation_ids.contains(observation_id.as_str()))
        })
        .collect::<Vec<_>>();
    let mut reprojected =
        PersonPageProjector::project(&affected_identity, &observations, &affected_supplementals);
    normalize_person_fact_ids(&mut reprojected, &fact_append_sequences)?;
    reprojected.profiles.retain(|profile| {
        next_consents.get(profile.person_id.as_str()) != Some(&ConsentStatus::OptedOut)
    });
    reprojected.slides.retain(|slide| {
        next_consents.get(slide.person_id.as_str()) != Some(&ConsentStatus::OptedOut)
    });
    reprojected.messages.retain(|message| {
        next_consents.get(message.person_id.as_str()) != Some(&ConsentStatus::OptedOut)
    });
    reprojected.activities.retain(|activity| {
        next_consents.get(activity.person_id.as_str()) != Some(&ConsentStatus::OptedOut)
    });

    let mut existing_items = BTreeMap::new();
    for owner in &affected_current_people {
        for item in lookup.person_message_items(owner)? {
            let message = person_message_from_projection_item(&item)?;
            if item.owner_key != *owner || message.person_id.as_str() != owner {
                return Err(SelfHostError::Ingestion(format!(
                    "projection item {} was returned for the wrong owner {owner}",
                    item.item_key
                )));
            }
            if existing_items.insert(item.item_key.clone(), item).is_some() {
                return Err(SelfHostError::Ingestion(format!(
                    "affected component contains duplicate person message item {}",
                    message.id
                )));
            }
        }
    }

    let reprojected_messages = std::mem::take(&mut reprojected.messages);
    let mut desired_items = BTreeMap::new();
    for message in &reprojected_messages {
        let item = person_message_projection_item(message)?;
        if desired_items.insert(item.item_key.clone(), item).is_some() {
            return Err(SelfHostError::Ingestion(format!(
                "component re-projection produced duplicate message {}",
                message.id
            )));
        }
    }

    let mut message_inserts = Vec::new();
    let mut message_updates = Vec::new();
    for (item_key, item) in desired_items {
        if existing_items.remove(&item_key).is_some() {
            // Replaced owners publish their complete desired row set. Keeping
            // equal rows in `updates` lets validation prove fact coverage
            // without loading unrelated owners.
            message_updates.push(item);
        } else {
            message_inserts.push(item);
        }
    }
    let message_delete_owners = existing_items
        .iter()
        .map(|(item_key, item)| (item_key.clone(), item.owner_key.clone()))
        .collect::<BTreeMap<_, _>>();
    let message_deletes = existing_items.into_keys().collect::<Vec<_>>();

    let mut person_page = current_page.clone();
    person_page
        .profiles
        .retain(|profile| !affected_current_people.contains(profile.person_id.as_str()));
    person_page
        .slides
        .retain(|slide| !affected_current_people.contains(slide.person_id.as_str()));
    person_page
        .activities
        .retain(|activity| !affected_current_people.contains(activity.person_id.as_str()));
    person_page.profiles.extend(reprojected.profiles);
    person_page.slides.extend(reprojected.slides);
    person_page.activities.extend(reprojected.activities);

    sort_person_page_for_identity(&mut person_page, &next_resolution.identity)?;

    let next_person_ids = next_resolution
        .identity
        .resolved_persons
        .iter()
        .map(|person| person.person_id.as_str())
        .collect::<BTreeSet<_>>();
    for owner in person_page
        .profiles
        .iter()
        .map(|profile| profile.person_id.as_str())
        .chain(
            person_page
                .slides
                .iter()
                .map(|slide| slide.person_id.as_str()),
        )
        .chain(
            person_page
                .activities
                .iter()
                .map(|activity| activity.person_id.as_str()),
        )
    {
        if !next_person_ids.contains(owner) {
            return Err(SelfHostError::Ingestion(format!(
                "component re-projection retained row for removed person {owner}"
            )));
        }
    }

    let replaced_person_owners = affected_current_people
        .union(&affected_next_people)
        .cloned()
        .collect();
    Ok(IncrementalPersonPageResult {
        person_page,
        message_inserts,
        message_updates,
        message_deletes,
        message_delete_owners,
        replaced_person_owners,
    })
}

fn affected_identity_members(
    current: &CompactIdentityResolution,
    next: &CompactIdentityResolution,
    current_consents: &BTreeMap<String, ConsentStatus>,
    next_consents: &BTreeMap<String, ConsentStatus>,
    touched_members: &BTreeSet<u64>,
) -> BTreeSet<u64> {
    let mut affected = touched_members.clone();

    let current_by_signature = component_people_by_signature(&current.members_by_person);
    let next_by_signature = component_people_by_signature(&next.members_by_person);
    for (signature, current_person) in &current_by_signature {
        if let Some(next_person) = next_by_signature.get(signature)
            && (current_person != next_person
                || current_consents.get(current_person) != next_consents.get(next_person))
        {
            affected.extend(signature.iter().copied());
        }
    }

    let current_owners = identity_identifier_owners(&current.identity);
    let next_owners = identity_identifier_owners(&next.identity);
    for identifier in current_owners.keys().chain(next_owners.keys()) {
        let current_owner = current_owners.get(identifier);
        let next_owner = next_owners.get(identifier);
        if current_owner != next_owner {
            if let Some(owner) = current_owner
                && let Some(members) = current.members_by_person.get(owner)
            {
                affected.extend(members.iter().copied());
            }
            if let Some(owner) = next_owner
                && let Some(members) = next.members_by_person.get(owner)
            {
                affected.extend(members.iter().copied());
            }
        }
    }

    loop {
        let before = affected.len();
        for members in current
            .members_by_person
            .values()
            .chain(next.members_by_person.values())
        {
            if members.iter().any(|member| affected.contains(member)) {
                affected.extend(members.iter().copied());
            }
        }
        if affected.len() == before {
            break;
        }
    }
    affected
}

fn component_people_by_signature(
    members_by_person: &BTreeMap<String, BTreeSet<u64>>,
) -> BTreeMap<Vec<u64>, String> {
    members_by_person
        .iter()
        .map(|(person_id, members)| {
            (
                members.iter().copied().collect::<Vec<_>>(),
                person_id.clone(),
            )
        })
        .collect()
}

fn component_people_intersecting(
    members_by_person: &BTreeMap<String, BTreeSet<u64>>,
    affected_members: &BTreeSet<u64>,
) -> BTreeSet<String> {
    members_by_person
        .iter()
        .filter(|(_, members)| {
            members
                .iter()
                .any(|member| affected_members.contains(member))
        })
        .map(|(person_id, _)| person_id.clone())
        .collect()
}

fn sort_person_page_for_identity(
    person_page: &mut PersonPageOutput,
    identity: &IdentityResolutionOutput,
) -> Result<(), SelfHostError> {
    let identity_order = identity
        .resolved_persons
        .iter()
        .enumerate()
        .map(|(index, person)| (person.person_id.as_str().to_owned(), index))
        .collect::<BTreeMap<_, _>>();
    let rank = |person_id: &str| {
        identity_order.get(person_id).copied().ok_or_else(|| {
            SelfHostError::Ingestion(format!(
                "person-page row references unknown person {person_id}"
            ))
        })
    };
    person_page
        .profiles
        .sort_by_key(|profile| rank(profile.person_id.as_str()).unwrap_or(usize::MAX));
    person_page
        .activities
        .sort_by_key(|activity| rank(activity.person_id.as_str()).unwrap_or(usize::MAX));

    let mut slide_sort_keys = BTreeMap::new();
    for slide in &person_page.slides {
        let key = (
            rank(slide.person_id.as_str())?,
            person_slide_append_seq(slide)?,
        );
        if slide_sort_keys.insert(slide.id.clone(), key).is_some() {
            return Err(SelfHostError::Ingestion(format!(
                "person-page contains duplicate slide id {}",
                slide.id
            )));
        }
    }
    person_page
        .slides
        .sort_by_key(|slide| slide_sort_keys[&slide.id]);
    Ok(())
}

fn identity_identifier_owners(identity: &IdentityResolutionOutput) -> BTreeMap<String, String> {
    let mut owners = BTreeMap::new();
    for person in &identity.resolved_persons {
        for identifier in &person.identifiers {
            owners.insert(
                identifier.value.clone(),
                person.person_id.as_str().to_owned(),
            );
        }
    }
    owners
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
) -> Result<ProjectionItem, SelfHostError> {
    let item = ProjectionItem {
        item_key: message.id.clone(),
        owner_key: message.person_id.as_str().to_owned(),
        sort_key: person_message_sort_key(message)?,
        value: serde_json::to_value(message)?,
    };
    item.validate()?;
    Ok(item)
}

pub(super) fn person_message_from_projection_item(
    item: &ProjectionItem,
) -> Result<PersonMessage, SelfHostError> {
    item.validate()?;
    let message: PersonMessage = serde_json::from_value(item.value.clone())?;
    if serde_json::to_value(&message)? != item.value {
        return Err(SelfHostError::Ingestion(format!(
            "projection item {} contains a non-canonical person message value",
            item.item_key
        )));
    }
    if item.item_key != message.id
        || item.owner_key != message.person_id.as_str()
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
) -> Result<Vec<ProjectionItem>, SelfHostError> {
    let messages = std::mem::take(&mut person_page.messages);
    let mut items = messages
        .iter()
        .map(person_message_projection_item)
        .collect::<Result<Vec<_>, _>>()?;
    items.sort_by(|left, right| {
        left.owner_key
            .cmp(&right.owner_key)
            .then_with(|| left.sort_key.cmp(&right.sort_key))
            .then_with(|| left.item_key.cmp(&right.item_key))
    });
    Ok(items)
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

#[cfg(test)]
fn detach_projection_items(
    snapshot: &mut ProjectionSnapshot,
) -> Result<(ProjectionItemCommit, u64, u64), SelfHostError> {
    let mut items = detach_person_messages(&mut snapshot.person_page)?;
    let person_message_count = u64::try_from(items.len()).map_err(|_| {
        SelfHostError::Ingestion(
            "person message item count does not fit u64 during full build".to_owned(),
        )
    })?;

    let mut expected_reply_slo = snapshot.reply_slo.clone();
    refresh_reply_slo_statuses(&mut expected_reply_slo, snapshot.built_at);
    if serde_json::to_value(&expected_reply_slo)? != serde_json::to_value(&snapshot.reply_slo)? {
        return Err(SelfHostError::Ingestion(
            "full reply SLO projection is internally inconsistent".to_owned(),
        ));
    }
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
    final_person_message_count: u64,
    final_reply_slo_count: u64,
    activities: &[PersonActivity],
) -> Result<(), SelfHostError> {
    pending.commit.validate()?;
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
    let (inserts, updates, deletes, replace) = match &pending.commit {
        ProjectionItemCommit::Replace { items } => {
            if pending.base_person_message_count != 0
                || pending.base_reply_slo_count != 0
                || !pending.replaced_person_owners.is_empty()
                || !pending.deleted_person_item_owners.is_empty()
            {
                return Err(SelfHostError::Ingestion(
                    "projection item replace commit must have zero base counts and owners"
                        .to_owned(),
                ));
            }
            (items.as_slice(), &[][..], &[][..], true)
        }
        ProjectionItemCommit::Delta {
            inserts,
            updates,
            deletes,
        } => (
            inserts.as_slice(),
            updates.as_slice(),
            deletes.as_slice(),
            false,
        ),
    };

    let mut inserted_person_items = Vec::new();
    let mut inserted_reply_slo_items = Vec::new();
    for item in inserts {
        if item.owner_key == REPLY_SLO_ITEM_OWNER {
            reply_slo_from_projection_item(item)?;
            inserted_reply_slo_items.push(item);
        } else {
            person_message_from_projection_item(item)?;
            inserted_person_items.push(item);
        }
    }
    let mut updated_person_items = Vec::new();
    for item in updates {
        if item.owner_key == REPLY_SLO_ITEM_OWNER {
            reply_slo_from_projection_item(item)?;
        } else {
            person_message_from_projection_item(item)?;
            if !pending.replaced_person_owners.contains(&item.owner_key) {
                return Err(SelfHostError::Ingestion(format!(
                    "person message update {} does not belong to a replaced owner",
                    item.item_key
                )));
            }
            updated_person_items.push(item);
        }
    }

    let delete_keys = deletes.iter().cloned().collect::<BTreeSet<_>>();
    let recorded_delete_keys = pending
        .deleted_person_item_owners
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if delete_keys != recorded_delete_keys {
        return Err(SelfHostError::Ingestion(
            "person message delete provenance does not match pending deletes".to_owned(),
        ));
    }
    let mut deleted_person_item_count = 0_u64;
    for item_key in deletes {
        if item_key.starts_with("reply-slo:") {
            return Err(SelfHostError::Ingestion(
                "observation projection delta must not delete reply SLO rows".to_owned(),
            ));
        }
        stable_fact_append_seq(item_key, "pm:")?;
        let owner = &pending.deleted_person_item_owners[item_key];
        if !pending.replaced_person_owners.contains(owner) {
            return Err(SelfHostError::Ingestion(format!(
                "person message delete {item_key} belongs to non-replaced owner {owner}"
            )));
        }
        deleted_person_item_count = deleted_person_item_count.checked_add(1).ok_or_else(|| {
            SelfHostError::Ingestion("pending person message delete count overflow".to_owned())
        })?;
    }

    let inserted_person_item_count = u64::try_from(inserted_person_items.len()).map_err(|_| {
        SelfHostError::Ingestion("pending person message item count does not fit u64".to_owned())
    })?;
    let inserted_reply_slo_item_count =
        u64::try_from(inserted_reply_slo_items.len()).map_err(|_| {
            SelfHostError::Ingestion("pending reply SLO item count does not fit u64".to_owned())
        })?;
    let expected_person_message_count = if replace {
        inserted_person_item_count
    } else {
        pending
            .base_person_message_count
            .checked_add(inserted_person_item_count)
            .and_then(|count| count.checked_sub(deleted_person_item_count))
            .ok_or_else(|| {
                SelfHostError::Ingestion("pending person message commit count overflow".to_owned())
            })?
    };
    if expected_person_message_count != final_person_message_count {
        return Err(SelfHostError::Ingestion(format!(
            "pending person message commit yields {expected_person_message_count} rows, expected {final_person_message_count}"
        )));
    }
    let expected_reply_slo_count = if replace {
        inserted_reply_slo_item_count
    } else {
        pending
            .base_reply_slo_count
            .checked_add(inserted_reply_slo_item_count)
            .ok_or_else(|| {
                SelfHostError::Ingestion("pending reply SLO commit count overflow".to_owned())
            })?
    };
    if expected_reply_slo_count != final_reply_slo_count {
        return Err(SelfHostError::Ingestion(format!(
            "pending reply SLO commit yields {expected_reply_slo_count} rows, expected {final_reply_slo_count}"
        )));
    }

    let mut append_sequences_by_owner = BTreeMap::<String, BTreeSet<u64>>::new();
    for item in inserted_person_items
        .iter()
        .chain(updated_person_items.iter())
    {
        let message = person_message_from_projection_item(item)?;
        let append_seq = person_message_append_seq(&message)?;
        if !activity_counts.contains_key(&item.owner_key) {
            return Err(SelfHostError::Ingestion(format!(
                "person message item {} references an owner with no activity",
                item.item_key
            )));
        }
        if !append_sequences_by_owner
            .entry(item.owner_key.clone())
            .or_default()
            .insert(append_seq)
        {
            return Err(SelfHostError::Ingestion(format!(
                "person message commit repeats append sequence {append_seq} for {}",
                item.owner_key
            )));
        }
    }

    for (owner, append_sequences) in append_sequences_by_owner {
        let final_owner_count = activity_counts[&owner];
        let committed_owner_count = u64::try_from(append_sequences.len()).map_err(|_| {
            SelfHostError::Ingestion(format!(
                "pending person message count does not fit u64 for {owner}"
            ))
        })?;
        if committed_owner_count > final_owner_count {
            return Err(SelfHostError::Ingestion(format!(
                "pending person message delta exceeds final activity count for {owner}"
            )));
        }
    }
    if replace {
        for (owner, count) in activity_counts {
            let committed = inserted_person_items
                .iter()
                .filter(|item| item.owner_key == owner)
                .count();
            if u64::try_from(committed).ok() != Some(count) {
                return Err(SelfHostError::Ingestion(format!(
                    "person message replace count does not match activity for {owner}"
                )));
            }
        }
    } else {
        for owner in &pending.replaced_person_owners {
            let expected = activity_counts.get(owner).copied().unwrap_or(0);
            let committed = inserted_person_items
                .iter()
                .chain(updated_person_items.iter())
                .filter(|item| item.owner_key == *owner)
                .count();
            if u64::try_from(committed).ok() != Some(expected) {
                return Err(SelfHostError::Ingestion(format!(
                    "component re-projection committed {committed} desired messages for {owner}, expected {expected}"
                )));
            }
        }
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

fn person_activity(
    person_id: &EntityRef,
    slides: &[lethe_projection_person::person_page::types::PersonSlide],
    messages: &[PersonMessage],
) -> PersonActivity {
    let mut active_channels = messages
        .iter()
        .map(|message| message.channel.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    active_channels.sort();
    let first_activity = slides
        .iter()
        .filter_map(|slide| slide.last_modified)
        .chain(messages.iter().map(|message| message.ts))
        .min();
    let last_activity = slides
        .iter()
        .filter_map(|slide| slide.last_modified)
        .chain(messages.iter().map(|message| message.ts))
        .max();
    PersonActivity {
        person_id: person_id.clone(),
        total_slides_related: slides.len(),
        total_messages: messages.len(),
        first_activity,
        last_activity,
        active_channels,
    }
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

fn refresh_reply_slo_statuses(projection: &mut ReplySloProjection, built_at: DateTime<Utc>) {
    for row in &mut projection.rows {
        row.latency_seconds = row
            .sent_at
            .map(|sent_at| (sent_at - row.published).num_seconds());
        row.status = match row.sent_at {
            Some(sent_at) if sent_at <= row.due_at => ReplySloStatus::SentOnTime,
            Some(_) => ReplySloStatus::SentLate,
            None if built_at > row.due_at => ReplySloStatus::Overdue,
            None => ReplySloStatus::Pending,
        };
    }
    projection.rows.sort_by(|left, right| {
        left.due_at.cmp(&right.due_at).then_with(|| {
            left.incoming_observation_id
                .as_str()
                .cmp(right.incoming_observation_id.as_str())
        })
    });
    projection.overdue = projection
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

fn person_page_output_count(
    person_page: &PersonPageOutput,
    person_message_count: u64,
) -> Result<usize, SelfHostError> {
    let person_message_count = usize::try_from(person_message_count).map_err(|_| {
        SelfHostError::Ingestion("person message output count does not fit usize".to_owned())
    })?;
    person_page
        .profiles
        .len()
        .checked_add(person_page.slides.len())
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
    let mut encoded = records
        .iter()
        .map(|record| Ok((record.id.as_str().to_owned(), serde_json::to_vec(record)?)))
        .collect::<Result<Vec<_>, SelfHostError>>()?;
    encoded.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hasher = Sha256::new();
    hasher.update(b"lethe:non-corpus-supplementals:v1\n");
    for (id, value) in encoded {
        hasher.update(id.as_bytes());
        hasher.update(b"\0");
        hasher.update(value);
        hasher.update(b"\n");
    }
    Ok(hex::encode(hasher.finalize()))
}

fn for_each_observation_page(
    persistence: &dyn StoragePorts,
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

        let mut observations = Vec::with_capacity(page.len());
        for stored in &page {
            if stored.append_seq <= after_append_seq {
                return Err(SelfHostError::Ingestion(format!(
                    "canonical observation page is not strictly ordered after append sequence {after_append_seq}"
                )));
            }
            if stored.append_seq > stats.max_append_seq {
                return Err(SelfHostError::Ingestion(format!(
                    "canonical observation page crossed fixed high-water {} at append sequence {}",
                    stats.max_append_seq, stored.append_seq
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
        visit(&page, &observations)?;
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
    persistence: &dyn StoragePorts,
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
                |(current_richness, current_created_at, _): &(
                    usize,
                    DateTime<Utc>,
                    FrontendProfile,
                )| {
                    richness > *current_richness
                        || (richness == *current_richness && created_at > *current_created_at)
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
    persistence: &dyn StoragePorts,
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

    for_each_observation_page(persistence, stats, page_size, |_, observations| {
        compact_state.apply_observation_page(observations)?;
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
    for_each_observation_page(persistence, stats, page_size, |stored, observations| {
        let fact_append_sequences = stored
            .iter()
            .map(|stored| (stored.observation.id.as_str().to_owned(), stored.append_seq))
            .collect::<BTreeMap<_, _>>();
        let mut inserts = Vec::new();
        if observations
            .iter()
            .any(|observation| observation.schema.as_str() == "schema:slack-message")
        {
            let delta = increment_person_page_for_slack(
                &identity,
                &identity,
                &person_page,
                &person_consents,
                &person_consents,
                observations,
                &fact_append_sequences,
            )?
            .ok_or_else(|| {
                SelfHostError::Ingestion(
                    "fixed-identity paged person projection unexpectedly changed topology"
                        .to_owned(),
                )
            })?;
            person_page = delta.person_page;
            if !delta.message_updates.is_empty()
                || !delta.message_deletes.is_empty()
                || !delta.message_delete_owners.is_empty()
                || !delta.replaced_person_owners.is_empty()
            {
                return Err(SelfHostError::Ingestion(
                    "fixed-identity paged projection produced a replacement delta".to_owned(),
                ));
            }
            inserts.extend(delta.message_inserts);
        }

        let non_slack = observations
            .iter()
            .filter(|observation| observation.schema.as_str() != "schema:slack-message")
            .cloned()
            .collect::<Vec<_>>();
        if !non_slack.is_empty() {
            let mut page = PersonPageProjector::project(&identity, &non_slack, &[]);
            normalize_person_fact_ids(&mut page, &fact_append_sequences)?;
            merge_non_slack_person_page(&mut person_page, page, &person_consents)?;
        }

        let reply_slo =
            ReplySloProjector::new(built_at).project_records(observations, supplementals);
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
        let page_person_message_count = inserts
            .len()
            .checked_sub(reply_slo.rows.len())
            .and_then(|count| u64::try_from(count).ok())
            .ok_or_else(|| {
                SelfHostError::Ingestion(
                    "paged person message row count does not fit u64".to_owned(),
                )
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

    let canonical_observation_fingerprint = hex::encode(canonical_fingerprint);
    let supplemental_fingerprint = supplemental_fingerprint(supplementals)?;
    let cognition_projector = CognitionStateProjector::new(built_at);
    let lineage = build_person_page_lineage(
        &canonical_observation_fingerprint,
        stats,
        &supplemental_fingerprint,
        supplementals.len(),
        person_page_output_count(&person_page, person_message_count)?,
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
        person_message_count,
        reply_slo_count,
        snapshot: ProjectionSnapshot {
            identity,
            person_page,
            answer_log,
            claim_queue: ClaimQueueProjector.project_records(supplementals),
            freshness,
            resume_snapshot: cognition_projector.resume_snapshot(supplementals),
            plan_state: cognition_projector.plan_state(supplementals),
            card_queue: CardQueueProjector::new(built_at).project_records(supplementals),
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
        &serde_json::to_value(&materialized)?,
        expected_item_count,
    )?;
    Ok(materialized)
}

fn materialized_snapshot_after_supplemental_delta(
    core: &AppCore,
    persistence: &dyn StoragePorts,
    changed: &lethe_core::domain::SupplementalRecord,
    built_at: DateTime<Utc>,
) -> Result<SupplementalMaterializedDelta, SelfHostError> {
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
    let persisted_materialized: MaterializedProjectionSnapshot =
        serde_json::from_value(persisted_manifest)?;
    persisted_materialized.validate()?;
    if !persisted_materialized.matches(persisted_stats, &core.supplemental_fingerprint)
        || persisted_materialized.canonical_observation_fingerprint
            != core.canonical_observation_fingerprint
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
    let supplementals = core
        .supplemental
        .list()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    if !supplementals.iter().any(|record| record.id == changed.id) {
        return Err(SelfHostError::Ingestion(format!(
            "supplemental {} is absent from resident state during delta materialization",
            changed.id
        )));
    }
    let next_supplemental_fingerprint = supplemental_fingerprint(&supplementals)?;
    if next_supplemental_fingerprint == core.supplemental_fingerprint {
        return Err(SelfHostError::Ingestion(format!(
            "supplemental {} did not advance the materialized fingerprint",
            changed.id
        )));
    }

    let mut snapshot = core.snapshot.clone();
    snapshot.freshness = freshness_projection_after_delta(
        &snapshot.freshness,
        &core.freshness_thresholds,
        &[],
        built_at,
    )?;
    snapshot.claim_queue = ClaimQueueProjector.project_records(&supplementals);
    let cognition_projector = CognitionStateProjector::new(built_at);
    snapshot.resume_snapshot = cognition_projector.resume_snapshot(&supplementals);
    snapshot.plan_state = cognition_projector.plan_state(&supplementals);
    snapshot.card_queue = CardQueueProjector::new(built_at).project_records(&supplementals);
    let frontend_profiles = frontend_profiles_from_supplementals(
        persistence,
        &snapshot.identity,
        &core.person_consents,
        &supplementals,
        core.observation_stats.max_append_seq,
    )?;
    install_frontend_profiles(&mut snapshot.person_page, frontend_profiles)?;

    let mut updates = Vec::new();
    if changed.kind == "send-record@1"
        && let Some(draft_id) = changed.derived_from.supplementals.first()
        && let Some(draft) = supplementals
            .iter()
            .find(|record| record.id == *draft_id && record.kind == "reply-draft@1")
        && let Some(observation_id) = draft.derived_from.observations.first()
    {
        let stored = persistence
            .observation_by_id(observation_id)?
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "reply draft {draft_id} references missing observation {observation_id}"
                ))
            })?;
        if stored.append_seq > core.observation_stats.max_append_seq {
            return Err(SelfHostError::Ingestion(format!(
                "reply draft {draft_id} crossed canonical high-water {}",
                core.observation_stats.max_append_seq
            )));
        }
        let projected = ReplySloProjector::new(built_at)
            .project_records(std::slice::from_ref(&stored.observation), &supplementals);
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

    snapshot.built_at = built_at;
    snapshot.lineage = build_person_page_lineage(
        &core.canonical_observation_fingerprint,
        core.observation_stats,
        &next_supplemental_fingerprint,
        supplementals.len(),
        person_page_output_count(&snapshot.person_page, core.person_message_count)?,
        built_at,
    );
    let materialized = MaterializedProjectionSnapshot {
        format_version: NON_CORPUS_MATERIALIZATION_VERSION,
        last_append_seq: core.observation_stats.max_append_seq,
        observation_count: core.observation_stats.count,
        canonical_observation_fingerprint: core.canonical_observation_fingerprint.clone(),
        supplemental_fingerprint: next_supplemental_fingerprint,
        compact_state: core.compact_state.clone(),
        person_consents: core.person_consents.clone(),
        person_message_count: core.person_message_count,
        reply_slo_count: core.reply_slo_count,
        snapshot,
        pending_item_commit: None,
    };
    materialized.validate()?;
    let item_commit = ProjectionItemCommit::Delta {
        inserts: Vec::new(),
        updates,
        deletes: Vec::new(),
    };
    item_commit.validate()?;
    Ok(SupplementalMaterializedDelta {
        materialized,
        item_commit,
    })
}

fn current_materialized_snapshot(
    value: serde_json::Value,
    stats: ObservationStats,
    supplemental_fingerprint: &str,
    persisted_projection_item_count: u64,
    persisted_reply_slo_count: u64,
) -> Result<Option<MaterializedProjectionSnapshot>, SelfHostError> {
    let format_version = value
        .as_object()
        .and_then(|object| object.get("format_version"))
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "proj:person-page materialization has no numeric format_version".to_owned(),
            )
        })?;
    if format_version != u64::from(NON_CORPUS_MATERIALIZATION_VERSION) {
        return Ok(None);
    }
    let materialized: MaterializedProjectionSnapshot = serde_json::from_value(value)?;
    materialized.validate()?;
    let expected_projection_item_count = materialized
        .person_message_count
        .checked_add(materialized.reply_slo_count)
        .ok_or_else(|| {
            SelfHostError::Ingestion(
                "proj:person-page manifest projection item count overflow".to_owned(),
            )
        })?;
    if expected_projection_item_count != persisted_projection_item_count {
        return Err(SelfHostError::Ingestion(format!(
            "proj:person-page manifest expects {expected_projection_item_count} projection item rows, but storage contains {persisted_projection_item_count}"
        )));
    }
    if materialized.reply_slo_count != persisted_reply_slo_count {
        return Err(SelfHostError::Ingestion(format!(
            "proj:person-page manifest expects {} reply SLO rows, but reserved owner contains {persisted_reply_slo_count}",
            materialized.reply_slo_count
        )));
    }
    Ok(materialized
        .matches(stats, supplemental_fingerprint)
        .then_some(materialized))
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

impl AppService {
    pub fn bootstrap(config: SelfHostConfig) -> Result<Self, SelfHostError> {
        let persistence = SqlitePersistence::open_with_routing_key_order(
            &config.database_path,
            &config.blob_dir,
            &config.secret_encryption_key,
            config.routing_key_order,
        )?;
        let stats = persistence.observation_stats()?;
        let supplementals = persistence.load_supplementals()?;
        validate_persisted_supplemental_anchors(&persistence, &supplementals)?;
        let supplemental_fingerprint = supplemental_fingerprint(&supplementals)?;
        let person_page_ref = ProjectionRef::new("proj:person-page");
        let persisted_projection_item_count =
            persistence.projection_item_count(&person_page_ref)?;
        let persisted_reply_slo_count =
            persistence.projection_item_count_by_owner(&person_page_ref, REPLY_SLO_ITEM_OWNER)?;
        let materialized = match persistence.projection_records(&person_page_ref)? {
            Some(value) => current_materialized_snapshot(
                value,
                stats,
                &supplemental_fingerprint,
                persisted_projection_item_count,
                persisted_reply_slo_count,
            )?,
            None => None,
        };
        let materialized = match materialized {
            Some(materialized) => materialized,
            None => rebuild_materialized_snapshot_paged(
                &persistence,
                &supplementals,
                &freshness_thresholds(&config),
                &config.channels,
                stats,
                config.corpus.rebuild_page_size,
                Utc::now(),
            )?,
        };
        let core = AppCore::from_materialized(
            materialized,
            Vec::new(),
            supplementals,
            freshness_thresholds(&config),
            config.channels.clone(),
        )?;
        let persistence: Arc<Mutex<Box<dyn StoragePorts>>> =
            Arc::new(Mutex::new(Box::new(persistence)));
        let corpus_config = config.corpus.projector_config();
        let search_index = search_index::SearchIndexManager::bootstrap(
            lethe_search_index::IndexRoot::new(
                &config.corpus.index_dir,
                config.corpus.writer_heap_bytes,
                corpus_config.fingerprint(),
            )?,
            CorpusProjector::new(corpus_config),
            config.corpus.rebuild_page_size,
            Arc::clone(&persistence),
        );
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

        Ok(Self {
            core: Arc::new(Mutex::new(core)),
            persistence,
            search_index,
            config: Arc::new(config),
            slack_sources,
            google_sources,
            slide_analyzer,
            resilient_executor: Arc::new(ResilientExecutor::new(
                3,
                std::time::Duration::from_secs(60),
            )),
            audit_log: Arc::new(InMemoryAuditLog::new()),
            #[cfg(test)]
            non_corpus_rebuild_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
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
            );
            return Err(SelfHostError::Auth("missing bearer token".to_string()));
        };
        let raw = header
            .to_str()
            .map_err(|_| SelfHostError::Auth("invalid authorization header".to_string()))?;
        let token = raw.strip_prefix("Bearer ").ok_or_else(|| {
            SelfHostError::Auth("authorization must use Bearer token".to_string())
        })?;
        let matched = self
            .config
            .api_tokens
            .iter()
            .find(|candidate| candidate.token.expose() == token)
            .ok_or_else(|| SelfHostError::Auth("token rejected".to_string()))?;
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
            );
            Ok(())
        } else {
            self.emit_audit(
                "actor:api-token",
                AuditEventKind::PolicyDenial,
                serde_json::json!({ "required_scopes": required_scopes, "reason": "scope denied" }),
            );
            Err(SelfHostError::Policy(format!(
                "token lacks required scopes {}",
                required_scopes.join(",")
            )))
        }
    }

    fn emit_audit(&self, actor: &str, kind: AuditEventKind, detail: serde_json::Value) {
        let event = AuditEvent {
            id: format!("audit:{}", uuid::Uuid::now_v7()),
            timestamp: Utc::now(),
            actor: ActorRef::new(actor),
            kind,
            detail,
        };
        match serde_json::to_string(&event) {
            Ok(json) => {
                if let Ok(store) = self.persistence.lock()
                    && let Err(error) = store.record_audit_event(
                        &event.id,
                        &event.timestamp.to_rfc3339(),
                        event.actor.as_str(),
                        &json,
                    )
                {
                    tracing::error!(error = %error, "failed to persist audit event");
                }
            }
            Err(error) => tracing::error!(error = %error, "failed to serialize audit event"),
        }
        self.audit_log.emit(event);
    }

    pub fn attribute_inventory_documents(
        &self,
    ) -> Result<Vec<AttributeInventoryDocument>, SelfHostError> {
        let core = self.core_lock()?;
        Ok(build_inventory_documents(&core.snapshot))
    }

    pub fn ingest_observation_drafts(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
    ) -> Result<ImportReport, SelfHostError> {
        if source_instance_id.trim().is_empty() {
            return Err(SelfHostError::Ingestion(
                "source_instance_id must not be blank".to_owned(),
            ));
        }

        let mut report = ImportReport {
            ingested: 0,
            duplicates: 0,
            quarantined: 0,
        };

        let mut core = self.core_lock()?;
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
            self.prepare_observation_draft_batch(
                &mut core,
                batch,
                source_instance_id,
                &mut prepared_observations,
            )?;
        }

        let outcomes = if prepared_observations.is_empty() {
            Vec::new()
        } else {
            self.persistence_lock()?
                .append_observations(&prepared_observations)
                .map_err(SelfHostError::Storage)?
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
                    request_appended_observations.push(observation);
                }
                DurableAppendOutcome::Duplicate(_) => report.duplicates += 1,
                DurableAppendOutcome::CanonicalCollision(_) => report.quarantined += 1,
            }
        }

        if !request_appended_observations.is_empty() {
            self.materialize_after_observation_append(&mut core, &request_appended_observations)?;
        }

        if report.ingested > 0 {
            self.emit_audit(
                "actor:self-host",
                AuditEventKind::WriteExecution,
                serde_json::json!({
                    "mode": "bulk_observation_import",
                    "source_instance_id": source_instance_id,
                    "ingested": report.ingested,
                    "duplicates": report.duplicates,
                    "quarantined": report.quarantined,
                }),
            );
            self.search_index.catch_up_after_append()?;
        }

        Ok(report)
    }

    fn prepare_observation_draft_batch(
        &self,
        core: &mut AppCore,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
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
                Ok(observation) => observations.push(observation),
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

mod media_support;
pub(crate) mod projection_api;
mod search_index;
mod service_support;
mod slide_support;
mod supplemental_write;
mod sync;
mod sync_support;

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
