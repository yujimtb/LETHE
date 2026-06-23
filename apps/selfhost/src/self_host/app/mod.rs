use std::collections::{BTreeSet, HashMap, HashSet};
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
    ObserverRef, ProjectionRef, ProjectionStatus, ReadMode, SchemaRef, SemVer, SourceSystemRef,
};
use lethe_derivation_gemini::{GeminiSlideAnalyzer, SlideAnalysisProjector};
use lethe_engine::identity::projector::IdentityProjector;
use lethe_engine::identity::types::IdentityResolutionOutput;
use lethe_engine::lake::{BlobStore, IngestRequest, IngestionGate, LakeStore};
use lethe_engine::projection::catalog::ProjectionCatalog;
use lethe_engine::projection::lineage::{LineageManifest, SourceSnapshot};
use lethe_engine::projection::runner::Projector;
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
use lethe_projection_corpus::{CorpusProjector, CorpusRecord};
use lethe_projection_person::person_page::projector::PersonPageProjector;
use lethe_projection_person::person_page::types::{
    PersonDetailResponse, PersonListItem, PersonPageOutput, TimelineEvent,
};
use lethe_storage_api::{AppendOutcome as DurableAppendOutcome, StorageError, StoragePorts};
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
    Adapter(#[from] AdapterError),
    #[error("read mode error: {0}")]
    ReadMode(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("policy denied: {0}")]
    Policy(String),
    #[error("authentication failed: {0}")]
    Auth(String),
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProjectionSnapshot {
    pub identity: IdentityResolutionOutput,
    pub person_page: PersonPageOutput,
    #[serde(default)]
    pub corpus: Vec<CorpusRecord>,
    #[serde(default)]
    pub answer_log: Vec<AnswerLogRecord>,
    pub built_at: DateTime<Utc>,
    pub lineage: LineageManifest,
}

impl Default for ProjectionSnapshot {
    fn default() -> Self {
        Self {
            identity: IdentityResolutionOutput::default(),
            person_page: PersonPageOutput::default(),
            corpus: Vec::new(),
            answer_log: Vec::new(),
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
    pub lake: LakeStore,
    pub blobs: BlobStore,
    pub supplemental: SupplementalStore,
    pub snapshot: ProjectionSnapshot,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub last_sync_error: Option<String>,
    pub sync_metrics: SyncMetrics,
}

impl AppCore {
    #[cfg(test)]
    fn new(
        observations: Vec<Observation>,
        persisted_blobs: Vec<Vec<u8>>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
    ) -> Result<Self, SelfHostError> {
        let mut lake = LakeStore::new();
        for observation in observations {
            lake.append(observation).map_err(|existing_id| {
                SelfHostError::Ingestion(format!(
                    "duplicate persisted observation detected during bootstrap: {existing_id}"
                ))
            })?;
        }

        let mut blobs = BlobStore::new();
        for blob in persisted_blobs {
            blobs.put(&blob);
        }

        let mut supplemental = SupplementalStore::new();
        for record in persisted_supplementals {
            supplemental.upsert(record, &lake).map_err(|err| {
                SelfHostError::Ingestion(format!(
                    "invalid persisted supplemental detected during bootstrap: {err}"
                ))
            })?;
        }

        let mut core = Self {
            registry: seed_registry(),
            catalog: seed_projection_catalog(),
            lake,
            blobs,
            supplemental,
            snapshot: ProjectionSnapshot::default(),
            last_sync_at: None,
            last_sync_error: None,
            sync_metrics: SyncMetrics::default(),
        };
        core.rebuild_snapshot();
        Ok(core)
    }

    fn rebuild_snapshot(&mut self) {
        let identity = IdentityProjector::new("1.0.0")
            .project(self.lake.list())
            .into_iter()
            .next()
            .unwrap_or_default();
        let supplemental_records = self.supplemental.by_kind("slide-analysis");
        let person_page =
            PersonPageProjector::project(&identity, self.lake.list(), &supplemental_records);
        let corpus = CorpusProjector::default_config().project_observations(self.lake.list());
        let answer_log = AnswerLogProjector.project_observations(self.lake.list());
        let built_at = Utc::now();
        let lineage = build_person_page_lineage(
            self.lake.list(),
            &supplemental_records,
            person_page.profiles.len()
                + person_page.slides.len()
                + person_page.messages.len()
                + person_page.activities.len(),
            built_at,
        );
        self.snapshot = ProjectionSnapshot {
            identity,
            person_page,
            corpus,
            answer_log,
            built_at,
            lineage,
        };
        self.catalog.set_status(
            &ProjectionRef::new("proj:identity-resolution"),
            ProjectionStatus::Active,
        );
        self.catalog.set_status(
            &ProjectionRef::new("proj:person-page"),
            ProjectionStatus::Active,
        );
        self.catalog
            .set_status(&ProjectionRef::new("proj:corpus"), ProjectionStatus::Active);
        self.catalog.set_status(
            &ProjectionRef::new("proj:answer-log"),
            ProjectionStatus::Active,
        );
    }

    fn empty_with_snapshot(snapshot: Option<ProjectionSnapshot>) -> Self {
        let mut core = Self {
            registry: seed_registry(),
            catalog: seed_projection_catalog(),
            lake: LakeStore::new(),
            blobs: BlobStore::new(),
            supplemental: SupplementalStore::new(),
            snapshot: snapshot.unwrap_or_default(),
            last_sync_at: None,
            last_sync_error: None,
            sync_metrics: SyncMetrics::default(),
        };
        core.catalog.set_status(
            &ProjectionRef::new("proj:identity-resolution"),
            ProjectionStatus::Active,
        );
        core.catalog.set_status(
            &ProjectionRef::new("proj:person-page"),
            ProjectionStatus::Active,
        );
        core.catalog
            .set_status(&ProjectionRef::new("proj:corpus"), ProjectionStatus::Active);
        core.catalog.set_status(
            &ProjectionRef::new("proj:answer-log"),
            ProjectionStatus::Active,
        );
        core
    }

    fn prepare_observation(
        &mut self,
        draft: ObservationDraft,
    ) -> Result<Observation, IngestResult> {
        let request = IngestRequest {
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
        };

        let gate = IngestionGate {
            registry: &self.registry,
            lake: &mut self.lake,
            blobs: &self.blobs,
        };
        gate.prepare_observation(request)
    }

    /// Upsert a supplemental record using this core's lake for validation.
    fn upsert_supplemental(
        &mut self,
        record: lethe_core::domain::SupplementalRecord,
    ) -> Result<lethe_engine::supplemental::store::UpsertRollback, lethe_core::domain::DomainError>
    {
        self.supplemental.upsert_with_rollback(record, &self.lake)
    }

    fn rollback_supplemental(
        &mut self,
        rollback: lethe_engine::supplemental::store::UpsertRollback,
    ) {
        self.supplemental.rollback_upsert(rollback);
    }
}

#[derive(Clone)]
pub struct AppService {
    core: Arc<Mutex<AppCore>>,
    persistence: Arc<Mutex<Box<dyn StoragePorts>>>,
    config: Arc<SelfHostConfig>,
    slack_sources: Vec<SlackSourceRuntime>,
    google_sources: Vec<GoogleSourceRuntime>,
    slide_analyzer: GeminiSlideAnalyzer,
    resilient_executor: Arc<ResilientExecutor>,
    audit_log: Arc<InMemoryAuditLog>,
}

impl ProjectionSnapshot {
    pub fn build(
        observations: Vec<Observation>,
        persisted_supplementals: Vec<lethe_core::domain::SupplementalRecord>,
    ) -> Result<Self, SelfHostError> {
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
        let identity = IdentityProjector::new("1.0.0")
            .project(lake.list())
            .into_iter()
            .next()
            .unwrap_or_default();
        let supplemental_records = supplemental.by_kind("slide-analysis");
        let person_page =
            PersonPageProjector::project(&identity, lake.list(), &supplemental_records);
        let corpus = CorpusProjector::default_config().project_observations(lake.list());
        let answer_log = AnswerLogProjector.project_observations(lake.list());
        let built_at = Utc::now();
        let lineage = build_person_page_lineage(
            lake.list(),
            &supplemental_records,
            person_page.profiles.len()
                + person_page.slides.len()
                + person_page.messages.len()
                + person_page.activities.len(),
            built_at,
        );
        Ok(Self {
            identity,
            person_page,
            corpus,
            answer_log,
            built_at,
            lineage,
        })
    }
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
        let persistence = SqlitePersistence::open(
            &config.database_path,
            &config.blob_dir,
            &config.secret_encryption_key,
        )?;
        let persisted_snapshot = lethe_storage_api::ProjectionMaterializer::projection_records(
            &persistence,
            &ProjectionRef::new("proj:person-page"),
        )?
        .map(serde_json::from_value)
        .transpose()?;
        if persisted_snapshot.is_none()
            && lethe_storage_api::ObservationStore::leaf_positions(&persistence)?
                .iter()
                .any(|position| position.append_seq > 0)
        {
            return Err(SelfHostError::Storage(StorageError::Invariant(
                "observations exist but proj:person-page materialization is missing; run an explicit rebuild"
                    .to_owned(),
            )));
        }
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
        let slide_analyzer =
            GeminiSlideAnalyzer::new(config.slide_ai.api_key.expose(), &config.slide_ai.model)?;

        Ok(Self {
            core: Arc::new(Mutex::new(AppCore::empty_with_snapshot(persisted_snapshot))),
            persistence: Arc::new(Mutex::new(Box::new(persistence))),
            config: Arc::new(config),
            slack_sources,
            google_sources,
            slide_analyzer,
            resilient_executor: Arc::new(ResilientExecutor::new(
                3,
                std::time::Duration::from_secs(60),
            )),
            audit_log: Arc::new(InMemoryAuditLog::new()),
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
}

mod media_support;
mod projection_api;
mod service_support;
mod slide_support;
mod sync;
mod sync_support;

use media_support::*;
#[cfg(test)]
use service_support::namespace_draft;
use service_support::{
    build_person_page_lineage, build_projection_lineage, consent_status_for_person_id,
};
use slide_support::*;
use sync_support::*;

#[cfg(test)]
mod tests;
