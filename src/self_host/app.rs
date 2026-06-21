use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::adapter::config::{
    AdapterConfig, BackoffStrategy, RateLimitConfig, RetryConfig, SchemaBinding,
};
use crate::adapter::gslides::client::GoogleSlidesClient;
use crate::adapter::gslides::mapper::GoogleSlidesAdapter;
use crate::adapter::slack::client::SlackClient;
use crate::adapter::slack::mapper::SlackAdapter;
use crate::adapter::traits::{ObservationDraft, SourceAdapter};
use crate::api::envelope::{ProjectionMetadata, ResponseEnvelope};
use crate::api::health::HealthResponse;
use crate::api::pagination::{PaginatedResponse, PaginationParams, paginate};
use crate::api::read_mode::{ReadModeError, ReadModeResolver};
use crate::attribute_inventory::{AttributeInventoryDocument, build_inventory_documents};
use crate::domain::{
    ActorRef, AuthorityModel, BlobRef, CaptureModel, EntityRef, IngestResult, Observation,
    ObserverRef, ProjectionRef, ProjectionStatus, ReadMode, SchemaRef, SemVer, SourceSystemRef,
};
use crate::governance::audit::{AuditLog, InMemoryAuditLog};
use crate::governance::engine::PolicyEngine;
use crate::governance::filter::FilteringGate;
use crate::governance::types::{
    AccessScope, AuditEvent, AuditEventKind, ConsentStatus, Environment, MaskStrategy, Operation,
    PolicyOutcome, PolicyRequest, RestrictedFieldSpec, Role,
};
use crate::identity::projector::IdentityProjector;
use crate::identity::types::IdentityResolutionOutput;
use crate::lake::{BlobStore, IngestRequest, IngestionGate, LakeStore};
use crate::person_page::projector::PersonPageProjector;
use crate::person_page::types::{
    PersonDetailResponse, PersonListItem, PersonPageOutput, TimelineEvent,
};
use crate::projection::catalog::ProjectionCatalog;
use crate::projection::lineage::{LineageManifest, SourceSnapshot};
use crate::projection::runner::Projector;
use crate::self_host::config::SelfHostConfig;
use crate::self_host::google::HttpGoogleSlidesClient;
use crate::self_host::persistence::{DurableAppendOutcome, PersistenceError, SqlitePersistence};
use crate::self_host::registry::{seed_projection_catalog, seed_registry};
use crate::self_host::slack::HttpSlackClient;
use crate::slide_analysis::GeminiSlideAnalyzer;
use crate::supplemental::SupplementalStore;

#[derive(Debug, thiserror::Error)]
pub enum SelfHostError {
    #[error(transparent)]
    Config(#[from] crate::self_host::config::ConfigError),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Adapter(#[from] crate::adapter::error::AdapterError),
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

#[derive(Debug, Clone)]
pub struct ProjectionSnapshot {
    pub identity: IdentityResolutionOutput,
    pub person_page: PersonPageOutput,
    pub built_at: DateTime<Utc>,
    pub lineage: LineageManifest,
}

impl Default for ProjectionSnapshot {
    fn default() -> Self {
        Self {
            identity: IdentityResolutionOutput::default(),
            person_page: PersonPageOutput::default(),
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
    pub registry: crate::registry::RegistryStore,
    pub catalog: ProjectionCatalog,
    pub lake: LakeStore,
    pub blobs: BlobStore,
    pub supplemental: SupplementalStore,
    pub snapshot: ProjectionSnapshot,
    pub last_sync_at: Option<DateTime<Utc>>,
    pub last_sync_error: Option<String>,
}

impl AppCore {
    fn new(
        observations: Vec<Observation>,
        persisted_blobs: Vec<Vec<u8>>,
        persisted_supplementals: Vec<crate::domain::SupplementalRecord>,
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
        record: crate::domain::SupplementalRecord,
    ) -> Result<crate::supplemental::store::UpsertRollback, crate::domain::DomainError> {
        self.supplemental.upsert_with_rollback(record, &self.lake)
    }

    fn rollback_supplemental(&mut self, rollback: crate::supplemental::store::UpsertRollback) {
        self.supplemental.rollback_upsert(rollback);
    }
}

#[derive(Clone)]
pub struct AppService {
    core: Arc<Mutex<AppCore>>,
    persistence: Arc<Mutex<SqlitePersistence>>,
    config: Arc<SelfHostConfig>,
    slack_client: HttpSlackClient,
    slack_replies_client: HttpSlackClient,
    google_client: HttpGoogleSlidesClient,
    slide_analyzer: GeminiSlideAnalyzer,
    audit_log: Arc<InMemoryAuditLog>,
}

impl AppService {
    pub fn bootstrap(config: SelfHostConfig) -> Result<Self, SelfHostError> {
        let persistence = SqlitePersistence::open(&config.database_path, &config.blob_dir)?;
        let observations = persistence.load_observations()?;
        let blobs = persistence.load_blobs()?;
        let supplementals = persistence.load_supplementals()?;
        let slack_client = HttpSlackClient::new(config.slack.bot_token.clone())?;
        let slack_replies_client = HttpSlackClient::new(config.slack.thread_token.clone())?;
        let google_client = HttpGoogleSlidesClient::new(&config.google)?;
        let slide_analyzer =
            GeminiSlideAnalyzer::new(&config.slide_ai.api_key, &config.slide_ai.model)?;

        Ok(Self {
            core: Arc::new(Mutex::new(AppCore::new(
                observations,
                blobs,
                supplementals,
            )?)),
            persistence: Arc::new(Mutex::new(persistence)),
            config: Arc::new(config),
            slack_client,
            slack_replies_client,
            google_client,
            slide_analyzer,
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
                    eprintln!("poll task join error: {err}");
                } else if let Ok(Err(err)) = result {
                    eprintln!("poll sync error: {err}");
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
        self.audit_log.emit(AuditEvent {
            id: format!("audit:{}", uuid::Uuid::now_v7()),
            timestamp: Utc::now(),
            actor: ActorRef::new(actor),
            kind,
            detail,
        });
    }

    pub fn attribute_inventory_documents(
        &self,
    ) -> Result<Vec<AttributeInventoryDocument>, SelfHostError> {
        let core = self.core_lock()?;
        Ok(build_inventory_documents(&core.snapshot))
    }

    pub fn sync_all(&self) -> Result<SyncReport, SelfHostError> {
        let mut slack_ingested = 0usize;
        let mut google_ingested = 0usize;
        let mut duplicates = 0usize;

        let slack_adapter =
            SlackAdapter::new(self.slack_client.clone(), self.slack_adapter_config());
        for channel_id in &self.config.slack.channel_ids {
            let cursor_key = format!("slack:{channel_id}:oldest_ts");
            let oldest = non_empty_state(self.persistence_lock()?.get_state(&cursor_key)?);
            let mut page_cursor: Option<String> = None;
            let mut latest_ts = oldest.clone();
            let mut thread_roots = self.known_thread_roots(channel_id)?;

            loop {
                let page = self.slack_client.conversations_history(
                    channel_id,
                    oldest.as_deref(),
                    page_cursor.as_deref(),
                    200,
                )?;
                for message in page.messages {
                    if let Some(thread_root) = thread_root_ts(&message) {
                        thread_roots.insert(thread_root.to_string());
                    }
                    match self.ingest_slack_message(
                        &slack_adapter,
                        &self.slack_client,
                        channel_id,
                        message,
                        &mut latest_ts,
                    )? {
                        IngestResult::Ingested { .. } => slack_ingested += 1,
                        IngestResult::Duplicate { .. } => duplicates += 1,
                        _ => {}
                    }
                }
                if page.has_more {
                    page_cursor = page.next_cursor;
                } else {
                    break;
                }
            }

            for thread_ts in thread_roots {
                let (ingested, dupes) =
                    self.sync_thread_replies(&slack_adapter, channel_id, &thread_ts)?;
                slack_ingested += ingested;
                duplicates += dupes;
            }

            let channel_snapshot = self.slack_client.conversations_info(channel_id)?;
            match self.ingest_draft(slack_adapter.map_channel_snapshot(&channel_snapshot))? {
                IngestResult::Ingested { .. } => slack_ingested += 1,
                IngestResult::Duplicate { .. } => duplicates += 1,
                _ => {}
            }

            if let Some(latest_ts) = latest_ts.as_deref() {
                self.persistence_lock()?.set_state(&cursor_key, latest_ts)?;
            }
        }

        match self.ingest_draft(slack_adapter.heartbeat())? {
            IngestResult::Ingested { .. } => slack_ingested += 1,
            IngestResult::Duplicate { .. } => duplicates += 1,
            _ => {}
        }

        let google_adapter =
            GoogleSlidesAdapter::new(self.google_client.clone(), self.google_adapter_config());
        for presentation_id in &self.config.google.presentation_ids {
            let cursor_key = format!("gslides:{presentation_id}:revision");
            let last_revision = self.persistence_lock()?.get_state(&cursor_key)?;

            let mut page_token: Option<String> = None;
            let mut revisions = Vec::new();
            loop {
                let page = self
                    .google_client
                    .list_revisions(presentation_id, page_token.as_deref())?;
                revisions.extend(page.revisions);
                if let Some(token) = page.next_page_token {
                    page_token = Some(token);
                } else {
                    break;
                }
            }
            revisions.sort_by_key(|revision| revision.modified_time);

            let should_reset = last_revision.as_ref().is_some_and(|needle| {
                !revisions
                    .iter()
                    .any(|revision| revision.revision_id == *needle)
            });
            let new_revisions =
                revisions_after_cursor(revisions, last_revision.as_deref(), should_reset);

            let Some(captured_revision) = latest_revision_to_capture(&new_revisions).cloned()
            else {
                continue;
            };

            let meta = self.google_client.get_presentation_meta(presentation_id)?;
            let presentation = self.google_client.get_presentation(presentation_id)?;
            let native_blob = self.store_blob(&serde_json::to_vec(&presentation)?)?;
            let rendered_blobs = presentation
                .slides
                .first()
                .map(|slide| {
                    self.google_client
                        .render_slide(presentation_id, &slide.object_id, "png")
                })
                .transpose()?
                .map(|rendered| self.store_blob(&rendered.data))
                .transpose()?
                .into_iter()
                .collect::<Vec<_>>();

            match self.ingest_draft(google_adapter.map_revision(
                &captured_revision,
                &meta,
                Some(native_blob),
                rendered_blobs,
            ))? {
                IngestResult::Ingested { .. } => google_ingested += 1,
                IngestResult::Duplicate { .. } => duplicates += 1,
                _ => {}
            }

            self.persistence_lock()?
                .set_state(&cursor_key, &captured_revision.revision_id)?;
        }

        match self.ingest_draft(google_adapter.heartbeat())? {
            IngestResult::Ingested { .. } => google_ingested += 1,
            IngestResult::Duplicate { .. } => duplicates += 1,
            _ => {}
        }

        let last_sync_at = Utc::now();
        let mut core = self.core_lock()?;
        core.last_sync_at = Some(last_sync_at);
        core.last_sync_error = None;
        let should_rebuild_snapshot = slack_ingested > 0 || google_ingested > 0;

        let schema = crate::domain::SchemaRef::new("schema:workspace-object-snapshot");
        let slide_observations: Vec<crate::domain::Observation> =
            core.lake.by_schema(&schema).into_iter().cloned().collect();
        let slide_obs_by_presentation = slide_observations.iter().fold(
            HashMap::<String, crate::domain::Observation>::new(),
            |mut acc, obs| {
                let Some(presentation_id) = obs
                    .payload
                    .pointer("/artifact/sourceObjectId")
                    .and_then(|value| value.as_str())
                else {
                    return acc;
                };

                match acc.get(presentation_id) {
                    Some(existing) if existing.published >= obs.published => {}
                    _ => {
                        acc.insert(presentation_id.to_string(), obs.clone());
                    }
                }
                acc
            },
        );
        let slide_analysis_records: Vec<crate::domain::SupplementalRecord> = core
            .supplemental
            .by_kind("slide-analysis")
            .into_iter()
            .cloned()
            .collect();
        let analysis_model = format!(
            "{}+continuation-v2-image-url",
            self.slide_analyzer.model_name()
        );
        let mut needs_analysis = false;
        for presentation_id in &self.config.google.presentation_ids {
            let Some(_observation) = slide_obs_by_presentation.get(presentation_id) else {
                continue;
            };
            let presentation = self.google_client.get_presentation(presentation_id)?;

            if presentation
                .slides
                .iter()
                .take(self.config.slide_analysis_limit)
                .any(|slide| {
                    match find_slide_analysis_record(
                        &slide_analysis_records,
                        presentation_id,
                        &slide.object_id,
                    ) {
                        Some(record) => analysis_record_needs_refresh(record, &analysis_model),
                        None => true,
                    }
                })
            {
                needs_analysis = true;
                break;
            }
        }

        // --- Slide Analysis ---
        let mut slide_analyses = 0usize;

        if google_ingested > 0 || slack_ingested > 0 || needs_analysis {
            let mut analysis_results = Vec::new();

            for presentation_id in &self.config.google.presentation_ids {
                let Some(observation) = slide_obs_by_presentation.get(presentation_id) else {
                    continue;
                };

                let presentation = self.google_client.get_presentation(presentation_id)?;
                let canonical_uri = observation
                    .payload
                    .pointer("/artifact/canonicalUri")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();

                let candidate_slide_indices = ranked_self_intro_slide_indices(
                    &presentation,
                    self.config.slide_analysis_limit,
                );
                let mut consumed_slide_indices = HashSet::new();

                for slide_index in candidate_slide_indices {
                    if !consumed_slide_indices.insert(slide_index) {
                        continue;
                    }

                    let slide = &presentation.slides[slide_index];
                    if let Some(existing) = find_slide_analysis_record(
                        &slide_analysis_records,
                        presentation_id,
                        &slide.object_id,
                    ) {
                        if !analysis_record_needs_refresh(existing, &analysis_model) {
                            continue;
                        }
                    }

                    let rendered = self.google_client.render_slide(
                        presentation_id,
                        &slide.object_id,
                        "png",
                    )?;
                    let thumbnail_blob_ref = core.blobs.put(&rendered.data);
                    self.persistence_lock()?.persist_blob(&rendered.data)?;
                    let Some(mut profile) = self.extract_student_profile_from_png(
                        &rendered.data,
                        observation,
                        &canonical_uri,
                    )?
                    else {
                        continue;
                    };
                    profile.normalize_in_place();

                    profile.source_slide_object_id = Some(slide.object_id.clone());
                    profile.source_document_id = Some(format!(
                        "document:gslides:{presentation_id}#slide:{}",
                        slide.object_id
                    ));
                    profile.source_canonical_uri = Some(canonical_uri.clone());
                    profile.thumbnail_blob_ref = Some(thumbnail_blob_ref.as_str().to_string());
                    profile.thumbnail_url = rendered.content_url.clone();
                    profile.companion_to_slide_object_id = None;
                    resolve_slide_image_urls(&presentation, slide, &mut profile);

                    let mut consumed_companion = false;
                    let mut companion_result = None;

                    if let Some(next_slide) = presentation.slides.get(slide_index + 1) {
                        let companion_rendered = self.google_client.render_slide(
                            presentation_id,
                            &next_slide.object_id,
                            "png",
                        )?;
                        let Some(mut companion_profile) = self.extract_student_profile_from_png(
                            &companion_rendered.data,
                            observation,
                            &canonical_uri,
                        )?
                        else {
                            continue;
                        };
                        companion_profile.normalize_in_place();

                        companion_profile.source_slide_object_id =
                            Some(next_slide.object_id.clone());
                        companion_profile.source_document_id = Some(format!(
                            "document:gslides:{presentation_id}#slide:{}",
                            next_slide.object_id
                        ));
                        companion_profile.source_canonical_uri = Some(canonical_uri.clone());
                        companion_profile.thumbnail_url = companion_rendered.content_url.clone();
                        companion_profile.companion_to_slide_object_id =
                            Some(slide.object_id.clone());
                        resolve_slide_image_urls(&presentation, next_slide, &mut companion_profile);

                        if should_merge_companion_slide(&profile, &companion_profile, observation) {
                            let companion_blob_ref = core.blobs.put(&companion_rendered.data);
                            self.persistence_lock()?
                                .persist_blob(&companion_rendered.data)?;
                            companion_profile.thumbnail_blob_ref =
                                Some(companion_blob_ref.as_str().to_string());
                            merge_companion_profile(&mut profile, &companion_profile);
                            consumed_companion = true;
                            consumed_slide_indices.insert(slide_index + 1);
                        }
                    }

                    ensure_profile_identifier(&mut profile, &slide.object_id);
                    profile.normalize_in_place();

                    let email = profile
                        .email
                        .as_deref()
                        .or(profile.generated_email.as_deref())
                        .map(ToOwned::to_owned)
                        .or_else(|| profile.source_document_id.clone())
                        .ok_or_else(|| {
                            SelfHostError::Ingestion(format!(
                                "slide analysis for {} produced no stable person identifier",
                                slide.object_id
                            ))
                        })?;
                    let person_entity = EntityRef::new(format!("person:{email}"));
                    analysis_results.push(crate::slide_analysis::types::SlideAnalysisResult {
                        source_observation_id: observation.id.clone(),
                        presentation_id: presentation_id.clone(),
                        profile: profile.clone(),
                        person_entity: person_entity.clone(),
                        supplemental_id: Some(crate::domain::SupplementalId::new(format!(
                            "sup:slide-analysis:{presentation_id}:{}",
                            slide.object_id
                        ))),
                        analyzed_at: observation.recorded_at,
                        model_version: Some(analysis_model.clone()),
                        slide_object_id: Some(slide.object_id.clone()),
                        thumbnail_blob_ref: Some(thumbnail_blob_ref),
                    });

                    if consumed_companion {
                        if let Some(next_slide) = presentation.slides.get(slide_index + 1) {
                            let mut companion_profile = profile.clone();
                            companion_profile.source_slide_object_id =
                                Some(next_slide.object_id.clone());
                            companion_profile.source_document_id = Some(format!(
                                "document:gslides:{presentation_id}#slide:{}",
                                next_slide.object_id
                            ));
                            companion_profile.companion_to_slide_object_id =
                                Some(slide.object_id.clone());
                            companion_profile.thumbnail_blob_ref = None;
                            companion_profile.profile_pic = None;
                            companion_result =
                                Some(crate::slide_analysis::types::SlideAnalysisResult {
                                    source_observation_id: observation.id.clone(),
                                    presentation_id: presentation_id.clone(),
                                    profile: companion_profile,
                                    person_entity,
                                    supplemental_id: Some(crate::domain::SupplementalId::new(
                                        format!(
                                            "sup:slide-analysis:{presentation_id}:{}",
                                            next_slide.object_id
                                        ),
                                    )),
                                    analyzed_at: observation.recorded_at,
                                    model_version: Some(analysis_model.clone()),
                                    slide_object_id: Some(next_slide.object_id.clone()),
                                    thumbnail_blob_ref: None,
                                });
                        }
                    }

                    if let Some(companion_result) = companion_result {
                        analysis_results.push(companion_result);
                    }
                }
            }

            slide_analyses = analysis_results.len();

            for result in &analysis_results {
                let record =
                    crate::slide_analysis::SlideAnalysisProjector::build_supplemental(result);
                let rollback = core
                    .upsert_supplemental(record)
                    .map_err(|err| SelfHostError::Ingestion(err.to_string()))?;
                let persisted_record =
                    core.supplemental
                        .get(&rollback.id)
                        .cloned()
                        .ok_or_else(|| {
                            SelfHostError::Ingestion(format!(
                                "supplemental {} missing after upsert",
                                rollback.id
                            ))
                        })?;
                if let Err(err) = self
                    .persistence_lock()?
                    .persist_supplemental(&persisted_record)
                {
                    core.rollback_supplemental(rollback);
                    return Err(SelfHostError::Persistence(err));
                }
            }

            for result in &analysis_results {
                let draft =
                    crate::slide_analysis::SlideAnalysisProjector::create_analysis_observation(
                        result,
                    );
                let observation = match core.prepare_observation(draft) {
                    Ok(observation) => observation,
                    Err(IngestResult::Rejected { message, .. }) => {
                        return Err(SelfHostError::Ingestion(message));
                    }
                    Err(IngestResult::Quarantined { ticket }) => {
                        return Err(SelfHostError::Ingestion(ticket.reason));
                    }
                    Err(result) => {
                        if let IngestResult::Duplicate { .. } = result {
                            continue;
                        }
                        return Err(SelfHostError::Ingestion(
                            "unexpected non-terminal ingestion result during slide analysis"
                                .to_owned(),
                        ));
                    }
                };
                if let IngestResult::Ingested { .. } =
                    self.append_prepared_observation(&mut core, observation)?
                {
                    // Count is derived from analysis_results; no per-row action needed here.
                }
            }
        }

        if should_rebuild_snapshot || slide_analyses > 0 {
            core.rebuild_snapshot();
        }

        Ok(SyncReport {
            slack_ingested,
            google_ingested,
            slide_analyses,
            duplicates,
            quarantined: 0,
            dead_letters: Vec::new(),
            last_sync_at,
        })
    }

    pub fn persons_response(
        &self,
        read_mode: Option<&str>,
        pin: Option<&str>,
        pagination: &PaginationParams,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new("projection:person-page"),
            ConsentStatus::RestrictedCapture,
        )?;

        let mut list: Vec<PersonListItem> = core
            .snapshot
            .person_page
            .profiles
            .iter()
            .filter_map(|profile| {
                let activity = core
                    .snapshot
                    .person_page
                    .activities
                    .iter()
                    .find(|activity| activity.person_id == profile.person_id)?;
                Some(PersonPageProjector::to_list_item(profile, activity))
            })
            .collect();
        list.sort_by(|left, right| right.last_activity.cmp(&left.last_activity));

        let (page, total) = paginate(&list, pagination);
        let payload = serde_json::to_value(PaginatedResponse::from_slice(page, total, pagination))?;

        Ok(ResponseEnvelope {
            data: self.apply_filter(payload),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_detail_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        let profile = core
            .snapshot
            .person_page
            .profiles
            .iter()
            .find(|profile| profile.person_id.as_str() == person_id)
            .ok_or_else(|| SelfHostError::NotFound(person_id.to_string()))?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let slides: Vec<_> = core
            .snapshot
            .person_page
            .slides
            .iter()
            .filter(|slide| slide.person_id == profile.person_id)
            .cloned()
            .collect();
        let messages: Vec<_> = core
            .snapshot
            .person_page
            .messages
            .iter()
            .filter(|message| message.person_id == profile.person_id)
            .cloned()
            .collect();
        let activity = core
            .snapshot
            .person_page
            .activities
            .iter()
            .find(|activity| activity.person_id == profile.person_id)
            .ok_or_else(|| SelfHostError::NotFound(format!("activity for {person_id}")))?;

        let detail: PersonDetailResponse =
            PersonPageProjector::to_detail(profile, &slides, &messages, activity);
        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(detail)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_slides_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let slides: Vec<_> = core
            .snapshot
            .person_page
            .slides
            .iter()
            .filter(|slide| slide.person_id.as_str() == person_id)
            .cloned()
            .collect();

        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(slides)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_messages_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let messages: Vec<_> = core
            .snapshot
            .person_page
            .messages
            .iter()
            .filter(|message| message.person_id.as_str() == person_id)
            .cloned()
            .collect();

        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(messages)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_timeline_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let mut events = Vec::new();

        for slide in core
            .snapshot
            .person_page
            .slides
            .iter()
            .filter(|slide| slide.person_id.as_str() == person_id)
        {
            if let Some(ts) = slide.last_modified {
                events.push(TimelineEvent {
                    event_type: "slide".into(),
                    document_id: Some(slide.document_id.clone()),
                    channel: None,
                    title: Some(slide.title.clone()),
                    text: None,
                    ts,
                });
            }
        }

        for message in core
            .snapshot
            .person_page
            .messages
            .iter()
            .filter(|message| message.person_id.as_str() == person_id)
        {
            events.push(TimelineEvent {
                event_type: "message".into(),
                document_id: None,
                channel: Some(message.channel.clone()),
                title: None,
                text: Some(message.text.clone()),
                ts: message.ts,
            });
        }

        events.sort_by(|left, right| right.ts.cmp(&left.ts));

        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(events)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn health(&self) -> Result<HealthResponse, SelfHostError> {
        let core = self.core_lock()?;
        Ok(HealthResponse::from_catalog(
            &core.catalog,
            env!("CARGO_PKG_VERSION"),
        ))
    }

    fn authorize_read(
        &self,
        target: EntityRef,
        consent_status: ConsentStatus,
    ) -> Result<(), SelfHostError> {
        let outcome = PolicyEngine::evaluate(&PolicyRequest {
            actor: ActorRef::new("actor:self-host"),
            role: Role::Researcher,
            operation: Operation::Read { target },
            data_scope: AccessScope::Restricted,
            consent_status,
            environment: Environment::Production,
        });

        match outcome {
            PolicyOutcome::Allow => Ok(()),
            PolicyOutcome::Deny { reason } => Err(SelfHostError::Policy(reason.message)),
            PolicyOutcome::RequireReview { route } => Err(SelfHostError::Policy(route.reason)),
        }
    }

    fn projection_metadata(
        &self,
        catalog: &ProjectionCatalog,
        projection_id: &str,
        read_mode: ReadMode,
        built_at: DateTime<Utc>,
        lineage: &LineageManifest,
    ) -> Result<ProjectionMetadata, SelfHostError> {
        let projection_id = ProjectionRef::new(projection_id);
        let entry = catalog
            .get(&projection_id)
            .ok_or_else(|| SelfHostError::NotFound(projection_id.to_string()))?;
        Ok(ProjectionMetadata {
            projection_id,
            version: entry.spec.version.clone(),
            built_at,
            read_mode,
            stale: false,
            lineage_ref: Some(lineage_ref(lineage)),
        })
    }

    pub fn lineage_manifest(&self, projection_id: &str) -> Result<LineageManifest, SelfHostError> {
        if projection_id != "proj:person-page" {
            return Err(SelfHostError::NotFound(projection_id.to_string()));
        }
        Ok(self.core_lock()?.snapshot.lineage.clone())
    }

    fn apply_filter(&self, payload: serde_json::Value) -> serde_json::Value {
        FilteringGate::filter(&payload, AccessScope::Internal, &restricted_fields()).payload
    }

    fn resolve_read_mode(
        &self,
        catalog: &ProjectionCatalog,
        projection_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ReadMode, SelfHostError> {
        let spec = &catalog
            .get(&ProjectionRef::new(projection_id))
            .ok_or_else(|| SelfHostError::NotFound(projection_id.to_string()))?
            .spec;
        ReadModeResolver::resolve(spec, read_mode, pin)
            .map_err(|err: ReadModeError| SelfHostError::ReadMode(err.to_string()))
    }

    fn ingest_draft(&self, draft: ObservationDraft) -> Result<IngestResult, SelfHostError> {
        let mut core = self.core_lock()?;
        let observation = match core.prepare_observation(draft) {
            Ok(observation) => observation,
            Err(IngestResult::Rejected { message, .. }) => {
                return Err(SelfHostError::Ingestion(message));
            }
            Err(IngestResult::Quarantined { ticket }) => {
                return Err(SelfHostError::Ingestion(ticket.reason));
            }
            Err(result) => return Ok(result),
        };

        let result = self.append_prepared_observation(&mut core, observation)?;

        match &result {
            IngestResult::Rejected { message, .. } => {
                Err(SelfHostError::Ingestion(message.clone()))
            }
            IngestResult::Quarantined { ticket } => {
                Err(SelfHostError::Ingestion(ticket.reason.clone()))
            }
            _ => Ok(result),
        }
    }

    fn append_prepared_observation(
        &self,
        core: &mut AppCore,
        observation: Observation,
    ) -> Result<IngestResult, SelfHostError> {
        let recorded_at = observation.recorded_at;

        let durable_outcome = self
            .persistence_lock()?
            .append_observation_idempotent(&observation)?;

        let result = match durable_outcome {
            DurableAppendOutcome::Appended(id) => match core.lake.append_idempotent(observation) {
                crate::lake::store::AppendOutcome::Appended(_) => {
                    IngestResult::Ingested { id, recorded_at }
                }
                crate::lake::store::AppendOutcome::Duplicate(existing_id)
                | crate::lake::store::AppendOutcome::Conflict(existing_id) => {
                    return Err(SelfHostError::Ingestion(format!(
                        "SQLite accepted observation {id}, but cache already contains {existing_id}"
                    )));
                }
            },
            DurableAppendOutcome::Duplicate(existing_id) => IngestResult::Duplicate { existing_id },
            DurableAppendOutcome::CanonicalCollision(existing_id) => IngestResult::Quarantined {
                ticket: crate::domain::QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    reason: format!(
                        "sha256-collision: existing observation {existing_id} has different canonical_json"
                    ),
                },
            },
        };
        Ok(result)
    }

    fn store_blob(&self, data: &[u8]) -> Result<BlobRef, SelfHostError> {
        let mut core = self.core_lock()?;
        let blob_ref = core.blobs.put(data);
        self.persistence_lock()?.persist_blob(data)?;
        Ok(blob_ref)
    }

    pub fn projection_blob_bytes(
        &self,
        blob_ref: &BlobRef,
    ) -> Result<Option<Vec<u8>>, SelfHostError> {
        let core = self.core_lock()?;
        let filtered_projection =
            self.apply_filter(serde_json::to_value(&core.snapshot.person_page)?);
        if !json_contains_string(&filtered_projection, blob_ref.as_str()) {
            return Ok(None);
        }
        Ok(core.blobs.get(blob_ref).map(|bytes| bytes.to_vec()))
    }

    fn ingest_slack_message(
        &self,
        slack_adapter: &SlackAdapter<HttpSlackClient>,
        file_client: &HttpSlackClient,
        channel_id: &str,
        mut message: crate::adapter::slack::client::SlackMessage,
        latest_ts: &mut Option<String>,
    ) -> Result<IngestResult, SelfHostError> {
        message.channel_id = channel_id.to_string();
        for file in &mut message.files {
            if file.blob_ref.is_none() {
                let data = file_client.file_download(file)?;
                let blob_ref = self.store_blob(&data)?;
                file.blob_ref = Some(blob_ref.as_str().to_string());
            }
        }
        let is_latest = match latest_ts.as_ref() {
            Some(current) => slack_ts_value(&message.ts)? > slack_ts_value(current)?,
            None => true,
        };
        if is_latest {
            *latest_ts = Some(message.ts.clone());
        }
        self.ingest_draft(slack_adapter.map_message(&message)?)
    }

    fn sync_thread_replies(
        &self,
        slack_adapter: &SlackAdapter<HttpSlackClient>,
        channel_id: &str,
        thread_ts: &str,
    ) -> Result<(usize, usize), SelfHostError> {
        let cursor_key = thread_cursor_key(channel_id, thread_ts);
        let reply_oldest = non_empty_state(self.persistence_lock()?.get_state(&cursor_key)?)
            .unwrap_or_else(|| thread_ts.to_string());
        let replies = self.slack_replies_client.conversations_replies(
            channel_id,
            thread_ts,
            Some(reply_oldest.as_str()),
        )?;
        let mut latest_reply_ts = Some(reply_oldest);
        let mut ingested = 0usize;
        let mut duplicates = 0usize;

        for reply in replies.into_iter().filter(|reply| reply.ts != thread_ts) {
            match self.ingest_slack_message(
                slack_adapter,
                &self.slack_replies_client,
                channel_id,
                reply,
                &mut latest_reply_ts,
            )? {
                IngestResult::Ingested { .. } => ingested += 1,
                IngestResult::Duplicate { .. } => duplicates += 1,
                _ => {}
            }
        }

        if let Some(latest_reply_ts) = latest_reply_ts.as_deref() {
            self.persistence_lock()?
                .set_state(&cursor_key, latest_reply_ts)?;
        }

        Ok((ingested, duplicates))
    }

    fn known_thread_roots(&self, channel_id: &str) -> Result<BTreeSet<String>, SelfHostError> {
        let core = self.core_lock()?;
        let observations: Vec<Observation> = core
            .lake
            .by_schema(&SchemaRef::new("schema:slack-message"))
            .into_iter()
            .cloned()
            .collect();
        Ok(known_thread_roots_from_observations(
            &observations,
            channel_id,
        ))
    }

    fn extract_student_profile_from_png(
        &self,
        image: &[u8],
        observation: &Observation,
        canonical_uri: &str,
    ) -> Result<Option<crate::slide_analysis::types::StudentProfile>, SelfHostError> {
        let title = observation
            .payload
            .get("title")
            .and_then(|value| value.as_str())
            .unwrap_or("Unknown");

        Ok(self
            .slide_analyzer
            .extract_profile_from_png(image, title, canonical_uri)?)
    }

    fn core_lock(&self) -> Result<std::sync::MutexGuard<'_, AppCore>, SelfHostError> {
        self.core.lock().map_err(|_| SelfHostError::LockPoisoned)
    }

    fn persistence_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, SqlitePersistence>, SelfHostError> {
        self.persistence
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)
    }

    fn slack_adapter_config(&self) -> AdapterConfig {
        AdapterConfig {
            observer_id: ObserverRef::new("obs:slack-crawler"),
            source_system_id: SourceSystemRef::new("sys:slack"),
            adapter_version: SemVer::new("1.0.0"),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            schemas: vec![
                SchemaRef::new("schema:slack-message"),
                SchemaRef::new("schema:slack-channel-snapshot"),
                SchemaRef::new("schema:observer-heartbeat"),
            ],
            schema_bindings: vec![SchemaBinding {
                schema: SchemaRef::new("schema:slack-message"),
                versions: ">=1.0.0 <2.0.0".into(),
            }],
            poll_interval: self.config.poll_interval,
            heartbeat_interval: self.config.poll_interval,
            rate_limit: RateLimitConfig {
                requests_per_second: 50,
                burst: 10,
            },
            retry: RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential,
                max_wait: self.config.poll_interval,
            },
            credential_ref: "env:LETHE_SLACK_BOT_TOKEN".into(),
        }
    }

    fn google_adapter_config(&self) -> AdapterConfig {
        AdapterConfig {
            observer_id: ObserverRef::new("obs:gslides-crawler"),
            source_system_id: SourceSystemRef::new("sys:google-slides"),
            adapter_version: SemVer::new("1.0.0"),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            schemas: vec![
                SchemaRef::new("schema:workspace-object-snapshot"),
                SchemaRef::new("schema:observer-heartbeat"),
            ],
            schema_bindings: vec![SchemaBinding {
                schema: SchemaRef::new("schema:workspace-object-snapshot"),
                versions: ">=1.0.0 <2.0.0".into(),
            }],
            poll_interval: self.config.poll_interval,
            heartbeat_interval: self.config.poll_interval,
            rate_limit: RateLimitConfig {
                requests_per_second: 10,
                burst: 5,
            },
            retry: RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential,
                max_wait: self.config.poll_interval,
            },
            credential_ref: "env:LETHE_GOOGLE_ACCESS_TOKEN".into(),
        }
    }
}

fn revisions_after_cursor(
    revisions: Vec<crate::adapter::gslides::client::SlideRevision>,
    cursor: Option<&str>,
    reset: bool,
) -> Vec<crate::adapter::gslides::client::SlideRevision> {
    if cursor.is_none() || reset {
        return revisions;
    }

    let cursor = cursor.unwrap();
    let mut found = false;
    revisions
        .into_iter()
        .filter(|revision| {
            if found {
                true
            } else if revision.revision_id == cursor {
                found = true;
                false
            } else {
                false
            }
        })
        .collect()
}

fn latest_revision_to_capture(
    revisions: &[crate::adapter::gslides::client::SlideRevision],
) -> Option<&crate::adapter::gslides::client::SlideRevision> {
    // The Google APIs used here only let us fetch the current presentation state,
    // so capturing anything older than the newest unseen revision would falsely
    // attach latest content to historical revision IDs.
    revisions.last()
}

fn thread_root_ts(message: &crate::adapter::slack::client::SlackMessage) -> Option<&str> {
    if message.reply_count == 0 {
        return None;
    }

    Some(message.thread_ts.as_deref().unwrap_or(message.ts.as_str()))
}

fn thread_cursor_key(channel_id: &str, thread_ts: &str) -> String {
    format!("slack:{channel_id}:thread:{thread_ts}:oldest_ts")
}

fn known_thread_roots_from_observations(
    observations: &[Observation],
    channel_id: &str,
) -> BTreeSet<String> {
    observations
        .iter()
        .filter_map(|observation| {
            if observation.schema.as_str() != "schema:slack-message" {
                return None;
            }

            if observation
                .payload
                .get("channel_id")
                .and_then(|value| value.as_str())
                != Some(channel_id)
            {
                return None;
            }

            let ts = observation
                .payload
                .get("ts")
                .and_then(|value| value.as_str())?;
            let thread_ts = observation
                .payload
                .get("thread_ts")
                .and_then(|value| value.as_str());
            let reply_count = observation
                .payload
                .get("reply_count")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);

            if thread_ts == Some(ts) || (thread_ts.is_none() && reply_count > 0) {
                return Some(ts.to_string());
            }

            None
        })
        .collect()
}

fn non_empty_state(value: Option<String>) -> Option<String> {
    value.filter(|raw| !raw.trim().is_empty())
}

fn restricted_fields() -> Vec<RestrictedFieldSpec> {
    [
        "identities",
        "DoB",
        "Birthplace",
        "dob",
        "birthplace",
        "email",
        "generated_email",
        "SNS",
    ]
    .into_iter()
    .map(|field_path| RestrictedFieldSpec {
        field_path: field_path.into(),
        level: AccessScope::Restricted,
        mask_strategy: MaskStrategy::Exclude,
    })
    .collect()
}

fn build_person_page_lineage(
    observations: &[Observation],
    supplementals: &[&crate::domain::SupplementalRecord],
    output_count: usize,
    built_at: DateTime<Utc>,
) -> LineageManifest {
    let mut observation_refs = observations
        .iter()
        .map(|observation| format!("observation:{}", observation.id))
        .collect::<Vec<_>>();
    let mut supplemental_refs = supplementals
        .iter()
        .map(|record| format!("supplemental:{}", record.id))
        .collect::<Vec<_>>();
    observation_refs.sort();
    supplemental_refs.sort();

    let mut hasher = Sha256::new();
    hasher.update(b"proj:person-page@1.0.0\n");
    for input_ref in observation_refs.iter().chain(&supplemental_refs) {
        hasher.update(input_ref.as_bytes());
        hasher.update(b"\n");
    }
    let build_id = format!("build-{}", hex::encode(hasher.finalize()));
    let mut lineage = LineageManifest::new(
        ProjectionRef::new("proj:person-page"),
        SemVer::new("1.0.0"),
        build_id,
    );
    lineage.built_at = built_at;
    lineage.output_count = output_count;
    lineage.deterministic = true;
    lineage.add_source(SourceSnapshot {
        source_ref: "lake".to_string(),
        watermark_position: Some(observations.len()),
        record_count: observations.len(),
    });
    lineage.add_source(SourceSnapshot {
        source_ref: "supplemental:slide-analysis".to_string(),
        watermark_position: None,
        record_count: supplementals.len(),
    });
    for input_ref in observation_refs.into_iter().chain(supplemental_refs) {
        lineage.add_input_ref(input_ref);
    }
    lineage
}

fn consent_status_for_person_id(
    core: &AppCore,
    person_id: &str,
) -> Result<ConsentStatus, SelfHostError> {
    let person = core
        .snapshot
        .identity
        .resolved_persons
        .iter()
        .find(|person| person.person_id.as_str() == person_id)
        .ok_or_else(|| SelfHostError::NotFound(person_id.to_string()))?;
    Ok(PersonPageProjector::consent_status_for_person(
        person,
        core.lake.list(),
    ))
}

fn json_contains_string(value: &serde_json::Value, needle: &str) -> bool {
    match value {
        serde_json::Value::String(value) => value == needle,
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, needle)),
        serde_json::Value::Object(values) => values
            .values()
            .any(|value| json_contains_string(value, needle)),
        _ => false,
    }
}

fn lineage_ref(lineage: &LineageManifest) -> String {
    format!(
        "lineage:{}:{}",
        lineage.projection_id.as_str().trim_start_matches("proj:"),
        lineage.build_id
    )
}

fn slack_ts_value(value: &str) -> Result<(i64, u32), SelfHostError> {
    let (seconds, fractional) = value.split_once('.').ok_or_else(|| {
        SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        ))
    })?;
    if fractional.len() != 6 || !fractional.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        )));
    }
    let seconds = seconds.parse::<i64>().map_err(|_| {
        SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        ))
    })?;
    let micros = fractional.parse::<u32>().map_err(|_| {
        SelfHostError::Ingestion(format!(
            "invalid Slack timestamp in persisted state: {value}"
        ))
    })?;
    Ok((seconds, micros))
}

fn analysis_record_needs_refresh(
    record: &crate::domain::SupplementalRecord,
    analysis_model: &str,
) -> bool {
    !analysis_record_is_rich(record) || record.model_version.as_deref() != Some(analysis_model)
}

fn should_merge_companion_slide(
    primary: &crate::slide_analysis::types::StudentProfile,
    companion: &crate::slide_analysis::types::StudentProfile,
    observation: &Observation,
) -> bool {
    if !profile_has_content(companion) {
        return false;
    }

    if companion
        .email
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return false;
    }

    let deck_title = observation
        .payload
        .get("title")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let primary_name = normalize_profile_name(&primary.name);
    let companion_name = normalize_profile_name(&companion.name);

    companion_name.is_empty()
        || companion_name == normalize_profile_name(deck_title)
        || (!primary_name.is_empty() && companion_name == primary_name)
}

fn profile_has_content(profile: &crate::slide_analysis::types::StudentProfile) -> bool {
    profile.has_meaningful_content() || profile.thumbnail_url.is_some()
}

fn normalize_profile_name(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_lowercase()
}

fn merge_companion_profile(
    primary: &mut crate::slide_analysis::types::StudentProfile,
    companion: &crate::slide_analysis::types::StudentProfile,
) {
    if let Some(companion_thumbnail_url) = &companion.thumbnail_url {
        let description = companion
            .bio_text
            .clone()
            .or_else(|| {
                companion
                    .profile_pic
                    .as_ref()
                    .and_then(|pic| pic.description.clone())
            })
            .or_else(|| Some("Continuation slide".to_string()));
        primary
            .gallery_images
            .push(crate::slide_analysis::types::GalleryImage {
                coordinates: None,
                description,
                url: Some(companion_thumbnail_url.clone()),
            });
    }

    primary
        .gallery_images
        .extend(companion.gallery_images.clone());

    if let Some(companion_bio) = companion
        .bio_text
        .as_ref()
        .map(|text| text.trim())
        .filter(|text| !text.is_empty())
    {
        match primary.bio_text.as_mut() {
            Some(primary_bio) if !primary_bio.contains(companion_bio) => {
                primary_bio.push_str("\n\n");
                primary_bio.push_str(companion_bio);
            }
            None => primary.bio_text = Some(companion_bio.to_string()),
            _ => {}
        }
    }

    if primary.profile_pic.is_none() {
        primary.profile_pic = companion.profile_pic.clone();
    }

    merge_optional_field(
        &mut primary.properties.nickname,
        &companion.properties.nickname,
    );
    merge_optional_field(
        &mut primary.properties.birthplace,
        &companion.properties.birthplace,
    );
    merge_optional_field(&mut primary.properties.dob, &companion.properties.dob);
    merge_optional_field(&mut primary.properties.major, &companion.properties.major);
    merge_optional_field(
        &mut primary.properties.affiliation,
        &companion.properties.affiliation,
    );
    merge_optional_field(&mut primary.properties.mbti, &companion.properties.mbti);
    merge_optional_field(&mut primary.properties.sns, &companion.properties.sns);
    merge_optional_field(
        &mut primary.properties.dislikes,
        &companion.properties.dislikes,
    );
    merge_optional_field(
        &mut primary.properties.new_challenges,
        &companion.properties.new_challenges,
    );
    merge_optional_field(
        &mut primary.properties.ask_me_about,
        &companion.properties.ask_me_about,
    );
    merge_optional_field(
        &mut primary.properties.turning_point,
        &companion.properties.turning_point,
    );
    merge_optional_field(&mut primary.properties.btw, &companion.properties.btw);
    merge_optional_field(
        &mut primary.properties.message,
        &companion.properties.message,
    );

    append_distinct_strings(
        &mut primary.properties.hobbies,
        &companion.properties.hobbies,
    );
    append_distinct_strings(
        &mut primary.properties.interests,
        &companion.properties.interests,
    );
    append_distinct_strings(&mut primary.properties.likes, &companion.properties.likes);
    append_distinct_strings(
        &mut primary.properties.hashtags,
        &companion.properties.hashtags,
    );
    append_distinct_strings(&mut primary.attributes, &companion.attributes);
}

fn resolve_slide_image_urls(
    presentation: &crate::adapter::gslides::client::PresentationNative,
    slide: &crate::adapter::gslides::client::SlideNative,
    profile: &mut crate::slide_analysis::types::StudentProfile,
) {
    let Some(page_size) = presentation.page_size.as_ref() else {
        return;
    };
    if page_size.width_emu <= 0 || page_size.height_emu <= 0 {
        return;
    }

    let mut available_images = slide_image_candidates(slide);
    if available_images.is_empty() {
        return;
    }

    if let Some(profile_pic) = profile.profile_pic.as_mut() {
        if let Some(coordinates) = profile_pic.coordinates.as_ref() {
            let target = normalize_coordinate_target(coordinates, page_size);
            if let Some(matched) = find_nearest_slide_image(target, &available_images) {
                let matched_object_id = matched.object_id.clone();
                let matched_url = apply_rotation_to_google_image_url(
                    &matched.content_url,
                    matched.rotation_degrees,
                );
                profile_pic.url = Some(matched_url);
                available_images.retain(|image| image.object_id != matched_object_id);
            }
        } else if profile_pic.url.is_none() {
            let first_image = available_images.remove(0);
            profile_pic.url = Some(apply_rotation_to_google_image_url(
                &first_image.content_url,
                first_image.rotation_degrees,
            ));
        }
    }

    for gallery_image in &mut profile.gallery_images {
        if gallery_image
            .url
            .as_deref()
            .is_some_and(|url| url.starts_with("http"))
        {
            continue;
        }
        let Some(coordinates) = gallery_image.coordinates.as_ref() else {
            continue;
        };
        let target = normalize_coordinate_target(coordinates, page_size);
        let Some(matched) = find_nearest_slide_image(target, &available_images) else {
            continue;
        };
        let matched_object_id = matched.object_id.clone();
        let matched_url =
            apply_rotation_to_google_image_url(&matched.content_url, matched.rotation_degrees);
        gallery_image.url = Some(matched_url);
        available_images.retain(|image| image.object_id != matched_object_id);
    }

    if profile.profile_pic.is_none() && !available_images.is_empty() {
        let first_image = available_images.remove(0);
        profile.profile_pic = Some(crate::slide_analysis::types::ProfilePic {
            coordinates: None,
            description: None,
            url: Some(apply_rotation_to_google_image_url(
                &first_image.content_url,
                first_image.rotation_degrees,
            )),
        });
    }
}

fn slide_image_candidates(
    slide: &crate::adapter::gslides::client::SlideNative,
) -> Vec<SlideImageCandidate> {
    slide
        .page_elements
        .iter()
        .enumerate()
        .filter_map(|(z_index, element)| slide_image_candidate_from_element(element, z_index))
        .collect()
}

fn slide_image_candidate_from_element(
    element: &serde_json::Value,
    z_index: usize,
) -> Option<SlideImageCandidate> {
    let image = element.get("image")?;
    let content_url = image.get("contentUrl")?.as_str()?.to_string();
    let object_id = element.get("objectId")?.as_str()?.to_string();
    let size = element.get("size")?;
    let width = size
        .get("width")
        .and_then(|value| value.get("magnitude"))
        .and_then(serde_json::Value::as_f64)?;
    let height = size
        .get("height")
        .and_then(|value| value.get("magnitude"))
        .and_then(serde_json::Value::as_f64)?;
    let transform = element.get("transform")?;
    let translate_x = transform
        .get("translateX")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default();
    let translate_y = transform
        .get("translateY")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default();
    let scale_x = transform
        .get("scaleX")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(1.0);
    let scale_y = transform
        .get("scaleY")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(1.0);
    let rotation_degrees = image
        .get("imageProperties")
        .and_then(|value| value.get("cropProperties"))
        .and_then(|_| Some(0))
        .unwrap_or(0);

    Some(SlideImageCandidate {
        object_id,
        content_url,
        center_x: translate_x + (width * scale_x.abs() / 2.0),
        center_y: translate_y + (height * scale_y.abs() / 2.0),
        z_index,
        rotation_degrees,
    })
}

fn normalize_coordinate_target(
    coordinates: &crate::slide_analysis::types::ImageCoordinates,
    page_size: &crate::adapter::gslides::client::PageSize,
) -> (f64, f64) {
    let x_pct = normalize_selection_percent(coordinates.x);
    let y_pct = normalize_selection_percent(coordinates.y);
    (
        (x_pct / 100.0) * page_size.width_emu as f64,
        (y_pct / 100.0) * page_size.height_emu as f64,
    )
}

fn normalize_selection_percent(value: f64) -> f64 {
    if value <= 100.0 {
        value.max(0.0)
    } else if value <= 1000.0 {
        (value / 10.0).max(0.0)
    } else {
        100.0
    }
}

fn find_nearest_slide_image(
    target: (f64, f64),
    candidates: &[SlideImageCandidate],
) -> Option<&SlideImageCandidate> {
    if candidates.is_empty() {
        return None;
    }
    let mut with_distance = candidates
        .iter()
        .map(|candidate| {
            let dx = candidate.center_x - target.0;
            let dy = candidate.center_y - target.1;
            let distance = (dx * dx + dy * dy).sqrt();
            (candidate, distance)
        })
        .collect::<Vec<_>>();
    let min_distance = with_distance
        .iter()
        .map(|(_, distance)| *distance)
        .fold(f64::INFINITY, f64::min);
    let tolerance = 50.0;
    with_distance.retain(|(_, distance)| *distance <= min_distance + tolerance);
    with_distance.sort_by(|left, right| {
        right
            .0
            .z_index
            .cmp(&left.0.z_index)
            .then_with(|| left.1.total_cmp(&right.1))
    });
    with_distance
        .into_iter()
        .map(|(candidate, _)| candidate)
        .next()
}

fn apply_rotation_to_google_image_url(url: &str, rotation_degrees: i32) -> String {
    if rotation_degrees == 0 || !url.contains("googleusercontent.com") {
        return url.to_string();
    }
    let mut parts = url.splitn(2, '?');
    let mut base = parts.next().unwrap_or_default().to_string();
    let query = parts
        .next()
        .map(|value| format!("?{value}"))
        .unwrap_or_default();
    if base.contains('=') {
        base.push_str(&format!("-r{rotation_degrees}"));
    } else {
        base.push_str(&format!("=r{rotation_degrees}"));
    }
    format!("{base}{query}")
}

fn merge_optional_field(target: &mut Option<String>, source: &Option<String>) {
    if target
        .as_ref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return;
    }
    *target = source.clone();
}

fn append_distinct_strings(target: &mut Vec<String>, source: &[String]) {
    for value in source {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

fn ensure_profile_identifier(
    profile: &mut crate::slide_analysis::types::StudentProfile,
    slide_object_id: &str,
) {
    if profile
        .email
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || profile
            .generated_email
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    {
        return;
    }

    let fallback = slide_object_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    profile.generated_email = Some(format!("slide-{fallback}@hlab.college"));
}

fn find_slide_analysis_record<'a>(
    records: &'a [crate::domain::SupplementalRecord],
    presentation_id: &str,
    slide_object_id: &str,
) -> Option<&'a crate::domain::SupplementalRecord> {
    records.iter().find(|record| {
        if record.kind != "slide-analysis" {
            return false;
        }
        let Ok(profile) = serde_json::from_value::<crate::slide_analysis::types::StudentProfile>(
            record.payload.clone(),
        ) else {
            return false;
        };
        profile.source_document_id.as_deref()
            == Some(&format!(
                "document:gslides:{presentation_id}#slide:{slide_object_id}"
            ))
            || profile.source_slide_object_id.as_deref() == Some(slide_object_id)
    })
}

fn analysis_record_is_rich(record: &crate::domain::SupplementalRecord) -> bool {
    let Ok(mut profile) = serde_json::from_value::<crate::slide_analysis::types::StudentProfile>(
        record.payload.clone(),
    ) else {
        return false;
    };
    profile.normalize_in_place();
    profile.has_meaningful_content()
}

fn audit_kind_for_scope(scope: &str) -> AuditEventKind {
    match scope {
        "admin:sync" => AuditEventKind::WriteExecution,
        "read:persons" | "read:timeline" => AuditEventKind::ReadRestricted,
        _ => AuditEventKind::ReadRestricted,
    }
}

fn ranked_self_intro_slide_indices(
    presentation: &crate::adapter::gslides::client::PresentationNative,
    limit: usize,
) -> Vec<usize> {
    let mut ranked = presentation
        .slides
        .iter()
        .enumerate()
        .map(|(index, slide)| {
            (
                index,
                score_self_intro_slide(slide, index, presentation.slides.len()),
            )
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));

    ranked
        .into_iter()
        .take(limit.min(presentation.slides.len()))
        .map(|(index, _)| index)
        .collect()
}

fn score_self_intro_slide(
    slide: &crate::adapter::gslides::client::SlideNative,
    index: usize,
    total_slides: usize,
) -> i32 {
    let fragments = extract_slide_text_fragments(slide);
    if fragments.is_empty() {
        return 0;
    }

    let text = fragments.join("\n").to_lowercase();
    let mut score = 0i32;

    if find_first_email(&fragments).is_some() {
        score += 8;
    }

    score += keyword_score(
        &text,
        &[
            "自己紹介",
            "self intro",
            "self-introduction",
            "about me",
            "profile",
            "プロフィール",
            "my name",
            "名前",
        ],
        6,
    );
    score += keyword_score(
        &text,
        &[
            "nickname",
            "ニックネーム",
            "mbti",
            "birthplace",
            "出身",
            "hobby",
            "hobbies",
            "趣味",
            "interest",
            "interests",
            "好き",
            "likes",
            "dislikes",
            "所属",
            "affiliation",
            "major",
            "学部",
            "学科",
            "message",
            "challenge",
            "turning point",
            "ask me",
        ],
        2,
    );
    score += keyword_score(&text, &["私", "ぼく", "僕", "俺", "i am", "i'm"], 1);
    score -= keyword_score(
        &text,
        &[
            "agenda",
            "project",
            "summary",
            "overview",
            "roadmap",
            "schedule",
            "目次",
            "進捗",
            "研究計画",
            "team",
        ],
        2,
    );

    if fragments.len() >= 3 {
        score += 2;
    }
    if slide_has_image_elements(slide) {
        score += 2;
    }

    let early_bonus = (total_slides.saturating_sub(index)).min(3) as i32;
    score + early_bonus
}

fn keyword_score(text: &str, keywords: &[&str], weight: i32) -> i32 {
    keywords
        .iter()
        .filter(|keyword| text.contains(**keyword))
        .count() as i32
        * weight
}

fn extract_slide_text_fragments(
    slide: &crate::adapter::gslides::client::SlideNative,
) -> Vec<String> {
    let mut fragments = Vec::new();
    for element in &slide.page_elements {
        collect_slide_text_values(element, None, &mut fragments);
    }

    let mut deduped = Vec::new();
    for fragment in fragments {
        let trimmed = fragment.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !deduped.iter().any(|existing: &String| existing == trimmed) {
            deduped.push(trimmed.to_string());
        }
    }
    deduped
}

fn collect_slide_text_values(
    value: &serde_json::Value,
    key: Option<&str>,
    fragments: &mut Vec<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (child_key, child_value) in map {
                collect_slide_text_values(child_value, Some(child_key.as_str()), fragments);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_slide_text_values(child, key, fragments);
            }
        }
        serde_json::Value::String(text)
            if matches!(key, Some("content") | Some("description") | Some("title")) =>
        {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                fragments.push(trimmed.to_string());
            }
        }
        _ => {}
    }
}

fn slide_has_image_elements(slide: &crate::adapter::gslides::client::SlideNative) -> bool {
    slide.page_elements.iter().any(|element| {
        element.get("image").is_some()
            || element
                .get("shape")
                .and_then(|shape| shape.get("shapeType"))
                .and_then(|value| value.as_str())
                == Some("RECTANGLE")
    })
}

fn find_first_email(fragments: &[String]) -> Option<String> {
    fragments.iter().find_map(|fragment| {
        fragment
            .split_whitespace()
            .map(|token| {
                token.trim_matches(|ch: char| {
                    matches!(ch, '<' | '>' | '(' | ')' | '[' | ']' | ',' | ';')
                })
            })
            .find(|token| token.contains('@') && token.contains('.'))
            .map(|token| token.to_lowercase())
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use crate::adapter::gslides::client::SlideRevision;
    use crate::adapter::slack::client::{SlackMessage, SlackMessageType};
    use crate::adapter::traits::ObservationDraft;
    use crate::domain::supplemental::InputAnchorSet;
    use chrono::Utc;

    use super::{
        AppCore, AppService, SelfHostError, extract_slide_text_fragments,
        known_thread_roots_from_observations, latest_revision_to_capture, non_empty_state,
        ranked_self_intro_slide_indices, thread_cursor_key, thread_root_ts,
    };
    use crate::domain::{
        ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
        ObserverRef, SchemaRef, SemVer, SupplementalId, SupplementalRecord,
    };
    use crate::governance::audit::InMemoryAuditLog;
    use crate::self_host::config::{
        ApiTokenConfig, GoogleConfig, ResourceLimits, SecretString, SelfHostConfig, SlackConfig,
        SlideAiConfig,
    };
    use crate::self_host::google::HttpGoogleSlidesClient;
    use crate::self_host::persistence::SqlitePersistence;
    use crate::self_host::slack::HttpSlackClient;
    use crate::slide_analysis::GeminiSlideAnalyzer;

    #[test]
    fn non_empty_state_filters_blank_values() {
        assert_eq!(non_empty_state(None), None);
        assert_eq!(non_empty_state(Some(String::new())), None);
        assert_eq!(non_empty_state(Some("   ".to_string())), None);
        assert_eq!(
            non_empty_state(Some("1234567890.123456".to_string())).as_deref(),
            Some("1234567890.123456")
        );
    }

    #[test]
    fn app_core_new_rejects_duplicate_persisted_observations() {
        fn observation(id: &str, key: &str) -> Observation {
            Observation {
                id: Observation::new_id(),
                schema: SchemaRef::new("schema:test"),
                schema_version: SemVer::new("1.0.0"),
                observer: ObserverRef::new("obs:test"),
                source_system: None,
                actor: None,
                authority_model: AuthorityModel::LakeAuthoritative,
                capture_model: CaptureModel::Event,
                subject: EntityRef::new(format!("entity:{id}")),
                target: None,
                payload: serde_json::json!({ "id": id }),
                attachments: vec![],
                published: Utc::now(),
                recorded_at: Utc::now(),
                consent: None,
                idempotency_key: IdempotencyKey::new(key),
                meta: serde_json::json!({
                    "canonical_json": serde_json::json!({
                        "source": "test",
                        "object_id": key,
                        "body": "duplicate"
                    }).to_string(),
                }),
            }
        }

        let observations = vec![observation("one", "dup-key"), observation("two", "dup-key")];

        let err = AppCore::new(observations, vec![], vec![]).unwrap_err();
        assert!(matches!(err, SelfHostError::Ingestion(_)));
    }

    #[test]
    fn latest_revision_to_capture_prefers_newest_revision() {
        let revisions = vec![
            SlideRevision {
                presentation_id: "pres-1".into(),
                revision_id: "rev-1".into(),
                modified_time: chrono::DateTime::parse_from_rfc3339("2026-03-24T10:00:00Z")
                    .unwrap()
                    .to_utc(),
                last_modifying_user: None,
            },
            SlideRevision {
                presentation_id: "pres-1".into(),
                revision_id: "rev-2".into(),
                modified_time: chrono::DateTime::parse_from_rfc3339("2026-03-24T11:00:00Z")
                    .unwrap()
                    .to_utc(),
                last_modifying_user: None,
            },
        ];

        assert_eq!(
            latest_revision_to_capture(&revisions).map(|revision| revision.revision_id.as_str()),
            Some("rev-2")
        );
    }

    fn test_config(db: PathBuf, blobs: PathBuf) -> SelfHostConfig {
        SelfHostConfig {
            bind_addr: "127.0.0.1:0".into(),
            database_path: db,
            blob_dir: blobs,
            poll_interval: std::time::Duration::from_secs(300),
            api_tokens: vec![ApiTokenConfig {
                token: SecretString::new("test-api-token").unwrap(),
                scopes: vec!["*".into()],
            }],
            resource_limits: ResourceLimits {
                max_blob_bytes: 10 * 1024 * 1024,
                max_payload_bytes: 1024 * 1024,
                max_sync_items: 10_000,
                max_page_size: 100,
            },
            slack: SlackConfig {
                bot_token: "xoxb-test-token".into(),
                thread_token: "xoxp-test-thread-token".into(),
                channel_ids: vec!["C01ABC".into()],
            },
            google: GoogleConfig {
                access_token: Some("ya29.test-token".into()),
                client_id: None,
                client_secret: None,
                refresh_token: None,
                presentation_ids: vec!["pres123".into()],
            },
            slide_analysis_limit: 10,
            slide_ai: SlideAiConfig {
                api_key: "test-gemini-key".into(),
                model: "test-gemini-model".into(),
            },
        }
    }

    #[test]
    fn thread_root_ts_returns_parent_thread_identifier() {
        let message = SlackMessage {
            channel_id: "C01ABC".into(),
            channel_name: "general".into(),
            ts: "1234567890.123456".into(),
            thread_ts: None,
            user_id: "U1".into(),
            user_name: "alice".into(),
            email: None,
            text: "hello".into(),
            message_type: SlackMessageType::Message,
            edited: None,
            reactions: vec![],
            files: vec![],
            reply_count: 2,
            reply_users_count: 1,
        };

        assert_eq!(thread_root_ts(&message), Some("1234567890.123456"));
    }

    #[test]
    fn thread_cursor_key_is_stable() {
        assert_eq!(
            thread_cursor_key("C01ABC", "1234567890.123456"),
            "slack:C01ABC:thread:1234567890.123456:oldest_ts"
        );
    }

    #[test]
    fn known_thread_roots_from_observations_finds_thread_parents() {
        fn slack_observation(
            channel_id: &str,
            ts: &str,
            thread_ts: Option<&str>,
            reply_count: Option<u64>,
        ) -> Observation {
            let mut payload = serde_json::json!({
                "channel_id": channel_id,
                "ts": ts,
                "text": "hello",
            });
            if let Some(thread_ts) = thread_ts {
                payload["thread_ts"] = serde_json::json!(thread_ts);
            }
            if let Some(reply_count) = reply_count {
                payload["reply_count"] = serde_json::json!(reply_count);
            }

            Observation {
                id: Observation::new_id(),
                schema: SchemaRef::new("schema:slack-message"),
                schema_version: SemVer::new("1.0.0"),
                observer: ObserverRef::new("obs:slack-crawler"),
                source_system: Some(crate::domain::SourceSystemRef::new("sys:slack")),
                actor: None,
                authority_model: AuthorityModel::LakeAuthoritative,
                capture_model: CaptureModel::Event,
                subject: EntityRef::new(format!("message:slack:{channel_id}:{ts}")),
                target: None,
                payload,
                attachments: vec![],
                published: Utc::now(),
                recorded_at: Utc::now(),
                consent: None,
                idempotency_key: IdempotencyKey::new(format!("slack:{channel_id}:{ts}")),
                meta: serde_json::json!({}),
            }
        }

        let roots = known_thread_roots_from_observations(
            &[
                slack_observation("C01ABC", "100.000001", None, Some(2)),
                slack_observation("C01ABC", "101.000001", Some("100.000001"), None),
                slack_observation("C02XYZ", "200.000001", None, Some(3)),
                slack_observation("C01ABC", "102.000001", None, Some(0)),
            ],
            "C01ABC",
        );

        assert_eq!(
            roots,
            std::collections::BTreeSet::from(["100.000001".to_string()])
        );
    }

    #[test]
    fn ranked_self_intro_slide_indices_prioritize_profile_like_slides() {
        let presentation = crate::adapter::gslides::client::PresentationNative {
            presentation_id: "deck-1".into(),
            title: "2026 Slides".into(),
            locale: None,
            slides: vec![
                crate::adapter::gslides::client::SlideNative {
                    object_id: "agenda".into(),
                    page_elements: vec![serde_json::json!({
                        "shape": {
                            "text": {
                                "textElements": [{ "textRun": { "content": "Agenda\n" } }]
                            }
                        }
                    })],
                },
                crate::adapter::gslides::client::SlideNative {
                    object_id: "profile".into(),
                    page_elements: vec![
                        serde_json::json!({
                            "shape": {
                                "text": {
                                    "textElements": [
                                        { "textRun": { "content": "自己紹介\n" } },
                                        { "textRun": { "content": "田中太郎\n" } },
                                        { "textRun": { "content": "tanaka@example.jp\n" } },
                                        { "textRun": { "content": "趣味: 写真\n" } }
                                    ]
                                }
                            }
                        }),
                        serde_json::json!({ "image": { "contentUrl": "https://example.com/pic.png" } }),
                    ],
                },
            ],
            page_size: None,
        };

        let ranked = ranked_self_intro_slide_indices(&presentation, 2);
        assert_eq!(ranked[0], 1);
    }

    #[test]
    fn ranked_self_intro_slide_indices_include_lower_scoring_slides_within_limit() {
        let presentation = crate::adapter::gslides::client::PresentationNative {
            presentation_id: "deck-2".into(),
            title: "2026 Slides".into(),
            locale: None,
            slides: vec![
                crate::adapter::gslides::client::SlideNative {
                    object_id: "profile".into(),
                    page_elements: vec![serde_json::json!({
                        "shape": {
                            "text": {
                                "textElements": [
                                    { "textRun": { "content": "自己紹介\n" } },
                                    { "textRun": { "content": "田中太郎\n" } }
                                ]
                            }
                        }
                    })],
                },
                crate::adapter::gslides::client::SlideNative {
                    object_id: "neutral".into(),
                    page_elements: vec![serde_json::json!({
                        "shape": {
                            "text": {
                                "textElements": [{ "textRun": { "content": "写真\n" } }]
                            }
                        }
                    })],
                },
            ],
            page_size: None,
        };

        let ranked = ranked_self_intro_slide_indices(&presentation, 2);
        assert_eq!(ranked, vec![0, 1]);
    }

    #[test]
    fn extract_slide_text_fragments_uses_text_runs() {
        let slide = crate::adapter::gslides::client::SlideNative {
            object_id: "profile".into(),
            page_elements: vec![serde_json::json!({
                "shape": {
                    "text": {
                        "textElements": [
                            { "textRun": { "content": "田中太郎\n" } },
                            { "textRun": { "content": "自己紹介\n" } }
                        ]
                    }
                }
            })],
        };

        let fragments = extract_slide_text_fragments(&slide);
        assert!(fragments.iter().any(|fragment| fragment == "田中太郎"));
    }

    #[test]
    fn ingest_draft_duplicate_is_decided_by_persistence_without_cache_append() {
        let root =
            std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
        let db = root.join("lethe.sqlite3");
        let blobs = root.join("blobs");
        let persistence = SqlitePersistence::open(&db, &blobs).unwrap();
        let persisted_observation = Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(crate::domain::SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("message:slack:existing"),
            target: None,
            payload: serde_json::json!({"text": "persisted"}),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("slack:C01ABC:dup-ts"),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({
                    "source": "slack",
                    "object_id": "channel:C01ABC:ts:dup-ts",
                    "body": "persisted"
                }).to_string(),
            }),
        };
        persistence
            .persist_observation(&persisted_observation)
            .unwrap();

        let config = test_config(db.clone(), blobs.clone());
        let service = AppService {
            core: Arc::new(Mutex::new(AppCore::new(vec![], vec![], vec![]).unwrap())),
            persistence: Arc::new(Mutex::new(persistence)),
            config: Arc::new(config.clone()),
            slack_client: HttpSlackClient::new(config.slack.bot_token.clone()).unwrap(),
            slack_replies_client: HttpSlackClient::new(config.slack.bot_token.clone()).unwrap(),
            google_client: HttpGoogleSlidesClient::new(&config.google).unwrap(),
            slide_analyzer: GeminiSlideAnalyzer::new(
                &config.slide_ai.api_key,
                &config.slide_ai.model,
            )
            .unwrap(),
            audit_log: Arc::new(InMemoryAuditLog::new()),
        };

        let draft = ObservationDraft {
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(crate::domain::SourceSystemRef::new("sys:slack")),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("message:slack:new"),
            target: None,
            payload: serde_json::json!({
                "channel_id": "C01ABC",
                "channel_name": "general",
                "ts": "dup-ts",
                "user_id": "U1",
                "user_name": "alice",
                "text": "new"
            }),
            attachments: vec![],
            published: Utc::now(),
            idempotency_key: IdempotencyKey::new("slack:C01ABC:dup-ts"),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({
                    "source": "slack",
                    "object_id": "channel:C01ABC:ts:dup-ts",
                    "body": "persisted"
                }).to_string(),
            }),
        };

        let result = service.ingest_draft(draft).unwrap();
        assert!(matches!(
            result,
            crate::domain::IngestResult::Duplicate { .. }
        ));
        assert_eq!(service.core_lock().unwrap().lake.len(), 0);
        assert_eq!(
            service
                .persistence_lock()
                .unwrap()
                .load_observations()
                .unwrap()
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn app_core_restores_persisted_slide_analysis_supplemental() {
        let observation = Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:workspace-object-snapshot"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:gslides-crawler"),
            source_system: Some(crate::domain::SourceSystemRef::new("sys:google-slides")),
            actor: None,
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new("document:gslides:pres123"),
            target: None,
            payload: serde_json::json!({
                "title": "自己紹介",
                "artifact": { "sourceObjectId": "pres123" },
                "relations": {
                    "owner": "tanaka@example.jp",
                    "editors": ["tanaka@example.jp"]
                }
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("gslides:pres123:rev:r1"),
            meta: serde_json::json!({}),
        };
        let supplemental = SupplementalRecord {
            id: SupplementalId::new("sup:slide-analysis:pres123:slide-1"),
            kind: "slide-analysis".into(),
            derived_from: InputAnchorSet {
                observations: vec![observation.id.clone()],
                blobs: vec![],
                supplementals: vec![],
            },
            payload: serde_json::json!({
                "name": "田中太郎",
                "bio_text": "私は田中太郎です",
                "source_slide_object_id": "slide-1",
                "source_document_id": "document:gslides:pres123#slide:slide-1"
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: Utc::now(),
            mutability: Mutability::ManagedCache,
            record_version: Some("1".into()),
            model_version: Some("fixture".into()),
            consent_metadata: None,
            lineage: None,
        };

        let core = AppCore::new(vec![observation], vec![], vec![supplemental]).unwrap();
        assert_eq!(
            core.snapshot.person_page.profiles[0]
                .self_intro_text
                .as_deref(),
            Some("私は田中太郎です")
        );
    }
}
