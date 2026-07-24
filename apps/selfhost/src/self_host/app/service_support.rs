use super::*;

struct BackgroundRebuildStorage<'a> {
    service: &'a AppService,
    started_at: Instant,
    page_count: std::sync::atomic::AtomicU64,
    max_lock_hold_ms: std::sync::atomic::AtomicU64,
}

struct BackgroundStorageSection<T> {
    value: T,
    lock_wait_ms: u64,
    lock_hold_ms: u64,
}

impl<'a> BackgroundRebuildStorage<'a> {
    fn new(service: &'a AppService) -> Self {
        Self {
            service,
            started_at: Instant::now(),
            page_count: std::sync::atomic::AtomicU64::new(0),
            max_lock_hold_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn duration_ms(duration: Duration) -> u64 {
        u64::try_from(duration.as_millis())
            .expect("background rebuild duration does not fit u64 milliseconds")
    }

    fn read<T>(
        &self,
        operation: &'static str,
        access: impl FnOnce(&dyn StoragePorts) -> Result<T, StorageError>,
    ) -> Result<BackgroundStorageSection<T>, SelfHostError> {
        let wait_started_at = Instant::now();
        let persistence = self.service.persistence_read_lock()?;
        let lock_wait_ms = Self::duration_ms(wait_started_at.elapsed());
        let hold_started_at = Instant::now();
        let result = access(persistence.as_ref()).map_err(SelfHostError::Storage);
        let lock_hold_ms = Self::duration_ms(hold_started_at.elapsed());
        drop(persistence);
        self.record_storage_section(operation, "read", lock_wait_ms, lock_hold_ms);
        Ok(BackgroundStorageSection {
            value: result?,
            lock_wait_ms,
            lock_hold_ms,
        })
    }

    fn write<T>(
        &self,
        operation: &'static str,
        access: impl FnOnce(&dyn StoragePorts) -> Result<T, StorageError>,
    ) -> Result<BackgroundStorageSection<T>, SelfHostError> {
        let wait_started_at = Instant::now();
        let persistence = self.service.persistence_lock()?;
        let lock_wait_ms = Self::duration_ms(wait_started_at.elapsed());
        let hold_started_at = Instant::now();
        let result = access(persistence.as_ref()).map_err(SelfHostError::Storage);
        let lock_hold_ms = Self::duration_ms(hold_started_at.elapsed());
        drop(persistence);
        self.record_storage_section(operation, "writer", lock_wait_ms, lock_hold_ms);
        Ok(BackgroundStorageSection {
            value: result?,
            lock_wait_ms,
            lock_hold_ms,
        })
    }

    fn record_storage_section(
        &self,
        operation: &'static str,
        lock_kind: &'static str,
        lock_wait_ms: u64,
        lock_hold_ms: u64,
    ) {
        self.max_lock_hold_ms
            .fetch_max(lock_hold_ms, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            operation,
            persistence_lock_kind = lock_kind,
            persistence_lock_wait_ms = lock_wait_ms,
            persistence_lock_hold_ms = lock_hold_ms,
            rebuild_elapsed_ms = Self::duration_ms(self.started_at.elapsed()),
            "background non-corpus rebuild persistence section completed"
        );
    }

    fn record_page(
        &self,
        page_kind: &'static str,
        rows: usize,
        lock_wait_ms: u64,
        lock_hold_ms: u64,
    ) {
        let page_number = self
            .page_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        #[cfg(test)]
        self.service
            .non_corpus_rebuild_page_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            page_kind,
            page_number,
            rows,
            elapsed_ms = Self::duration_ms(self.started_at.elapsed()),
            persistence_lock_wait_ms = lock_wait_ms,
            persistence_lock_hold_ms = lock_hold_ms,
            "background non-corpus rebuild page completed"
        );
        if page_number == 1 || page_number % 100 == 0 {
            tracing::info!(
                page_count = page_number,
                elapsed_ms = Self::duration_ms(self.started_at.elapsed()),
                max_persistence_lock_hold_ms = self
                    .max_lock_hold_ms
                    .load(std::sync::atomic::Ordering::Relaxed),
                "background non-corpus rebuild progress"
            );
        }
        #[cfg(test)]
        if let Some(delay) = self.service.non_corpus_rebuild_page_delay {
            std::thread::sleep(delay);
        }
    }

    fn emit_completion(&self, target: ObservationStats, current: ObservationStats) {
        tracing::info!(
            page_count = self.page_count.load(std::sync::atomic::Ordering::Relaxed),
            elapsed_ms = Self::duration_ms(self.started_at.elapsed()),
            max_persistence_lock_hold_ms = self
                .max_lock_hold_ms
                .load(std::sync::atomic::Ordering::Relaxed),
            target_count = target.count,
            target_append_seq = target.max_append_seq,
            current_count = current.count,
            current_append_seq = current.max_append_seq,
            "background non-corpus materialization completed"
        );
    }
}

impl NonCorpusRebuildStorage for BackgroundRebuildStorage<'_> {
    fn load_supplementals(&self) -> Result<Vec<SupplementalRecord>, SelfHostError> {
        Ok(self
            .read("load_supplementals", |storage| storage.load_supplementals())?
            .value)
    }

    fn observation_stats(&self) -> Result<ObservationStats, SelfHostError> {
        Ok(self
            .read("observation_stats", |storage| storage.observation_stats())?
            .value)
    }

    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError> {
        let section = self.read("observation_page", |storage| {
            storage.observation_page(after_append_seq, limit)
        })?;
        self.record_page(
            "canonical",
            section.value.len(),
            section.lock_wait_ms,
            section.lock_hold_ms,
        );
        Ok(section.value)
    }

    fn observations_for_privacy_key_page(
        &self,
        privacy_key: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, SelfHostError> {
        let section = self.read("privacy_key_page", |storage| {
            storage.observations_for_privacy_key_page(privacy_key, after_append_seq, limit)
        })?;
        self.record_page(
            "privacy",
            section.value.len(),
            section.lock_wait_ms,
            section.lock_hold_ms,
        );
        Ok(section.value)
    }

    fn observation_by_id(
        &self,
        id: &ObservationId,
    ) -> Result<Option<StoredObservation>, SelfHostError> {
        Ok(self
            .read("observation_by_id", |storage| storage.observation_by_id(id))?
            .value)
    }

    fn commit_projection_items(
        &self,
        projection: &ProjectionRef,
        manifest: &serde_json::Value,
        commit: &ProjectionItemCommit,
    ) -> Result<(), SelfHostError> {
        self.write("commit_projection_items", |storage| {
            storage.commit_projection_items(projection, manifest, commit)
        })?;
        Ok(())
    }

    fn projection_item_count_by_owner(
        &self,
        projection: &ProjectionRef,
        owner_key: &str,
    ) -> Result<u64, SelfHostError> {
        Ok(self
            .read("projection_item_count_by_owner", |storage| {
                storage.projection_item_count_by_owner(projection, owner_key)
            })?
            .value)
    }

    fn publish_projection_items_from_staging(
        &self,
        target: &ProjectionRef,
        staging: &ProjectionRef,
        manifest: &serde_json::Value,
        expected_item_count: u64,
    ) -> Result<(), SelfHostError> {
        self.write("publish_projection_items_from_staging", |storage| {
            storage.publish_projection_items_from_staging(
                target,
                staging,
                manifest,
                expected_item_count,
            )
        })?;
        Ok(())
    }

    fn set_state(&self, key: &str, value: &str) -> Result<(), SelfHostError> {
        self.write("set_state", |storage| storage.set_state(key, value))?;
        Ok(())
    }
}

impl AppService {
    pub fn mcp_oauth_config(&self) -> crate::self_host::config::McpOAuthConfig {
        self.config.mcp_oauth.clone()
    }

    pub(super) fn enforce_cutover_admission_for_import(
        &self,
        source_instance_id: &str,
        api_version: lethe_storage_api::CutoverApiVersion,
        generation: Option<u64>,
        timer: &mut ObservationImportTimer,
    ) -> Result<(), SelfHostError> {
        match self.persistence_lock_for_import(timer)?.cutover_admit(
            source_instance_id,
            api_version,
            generation,
        ) {
            Ok(()) => Ok(()),
            Err(StorageError::CutoverAdmissionDenied(detail)) => Err(SelfHostError::Auth(detail)),
            Err(error) => Err(SelfHostError::Storage(error)),
        }
    }

    pub fn apply_identity_bridge_batch(
        &self,
        batch_size: usize,
    ) -> Result<lethe_storage_api::IdentityBridgeBatchReport, SelfHostError> {
        Ok(self
            .persistence_lock()?
            .identity_bridge_apply_batch(batch_size)?)
    }

    pub(super) fn catch_up_identity_bridge(&self) -> Result<(), SelfHostError> {
        loop {
            let report = self.apply_identity_bridge_batch(16_384)?;
            if report.read_count == 0 {
                return Ok(());
            }
        }
    }

    pub fn identity_bridge_watermark(&self) -> Result<u64, SelfHostError> {
        Ok(self.persistence_read_lock()?.identity_bridge_watermark()?)
    }

    pub fn cutover_register_unit(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> Result<CutoverState, SelfHostError> {
        Ok(self
            .persistence_lock()?
            .cutover_register(source_instance_id, authority, reason)?)
    }

    pub fn cutover_inventory(&self) -> Result<Vec<CutoverInventoryItem>, SelfHostError> {
        Ok(self.persistence_read_lock()?.cutover_inventory()?)
    }

    pub fn cutover_state(&self, source_instance_id: &str) -> Result<CutoverState, SelfHostError> {
        Ok(self
            .persistence_read_lock()?
            .cutover_state(source_instance_id)?)
    }

    pub fn cutover_begin_drain(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> Result<CutoverState, SelfHostError> {
        let _operation = self.bulk_import_operation_lock()?;
        Ok(self
            .persistence_lock()?
            .cutover_begin_drain(source_instance_id, authority, reason)?)
    }

    pub fn cutover_readiness(
        &self,
        source_instance_id: &str,
        fixture: Option<&CutoverFixture>,
    ) -> Result<CutoverReadinessReport, SelfHostError> {
        Ok(self
            .persistence_read_lock()?
            .cutover_readiness(source_instance_id, fixture)?)
    }

    pub fn cutover_activate(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
        fixture: &CutoverFixture,
    ) -> Result<CutoverState, SelfHostError> {
        let _operation = self.bulk_import_operation_lock()?;
        Ok(self.persistence_lock()?.cutover_activate(
            source_instance_id,
            authority,
            reason,
            fixture,
        )?)
    }

    pub fn cutover_rollback(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> Result<CutoverState, SelfHostError> {
        let _operation = self.bulk_import_operation_lock()?;
        Ok(self
            .persistence_lock()?
            .cutover_rollback(source_instance_id, authority, reason)?)
    }

    pub fn cutover_health(&self, source_instance_id: &str) -> Result<CutoverHealth, SelfHostError> {
        Ok(self
            .persistence_read_lock()?
            .cutover_health(source_instance_id)?)
    }

    pub fn health(&self) -> Result<HealthResponse, SelfHostError> {
        let core = self.core_snapshot();
        let append_consumer = self.append_consumer_health_dependency()?;
        Ok(
            HealthResponse::from_catalog(&core.catalog, env!("CARGO_PKG_VERSION")).with_runtime(
                vec![
                    self.bulk_import_health_dependency()?,
                    self.search_index.health_dependency(),
                    append_consumer,
                ],
                LastSyncHealth {
                    completed_at: core.last_sync_at,
                    error: core.last_sync_error.clone(),
                },
                core.sync_metrics.clone(),
            ),
        )
    }

    pub fn deep_health(&self) -> Result<HealthResponse, SelfHostError> {
        let storage_dependency = match self.persistence_lock()?.deep_check() {
            Ok(()) => DependencyHealthInfo {
                name: "storage".to_owned(),
                status: "ok".to_owned(),
                detail: None,
            },
            Err(error) => DependencyHealthInfo {
                name: "storage".to_owned(),
                status: "failed".to_owned(),
                detail: Some(error.to_string()),
            },
        };
        let core = self.core_snapshot();
        let append_consumer = self.append_consumer_health_dependency()?;
        Ok(
            HealthResponse::from_catalog(&core.catalog, env!("CARGO_PKG_VERSION")).with_runtime(
                vec![
                    storage_dependency,
                    self.bulk_import_health_dependency()?,
                    self.search_index.deep_health_dependency(),
                    append_consumer,
                ],
                LastSyncHealth {
                    completed_at: core.last_sync_at,
                    error: core.last_sync_error.clone(),
                },
                core.sync_metrics.clone(),
            ),
        )
    }

    pub(super) fn authorize_read(
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

    pub(super) fn projection_metadata(
        &self,
        catalog: &ProjectionCatalog,
        projection_id: &str,
        read_mode: ReadMode,
        built_at: DateTime<Utc>,
        lineage: &LineageManifest,
    ) -> Result<ProjectionMetadata, SelfHostError> {
        self.ensure_projection_fresh(catalog, projection_id)?;
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
        let core = self.core_snapshot();
        self.ensure_projection_fresh(&core.catalog, projection_id)?;
        match projection_id {
            "proj:person-page" => Ok(core.snapshot.lineage.clone()),
            "proj:corpus" => {
                drop(core);
                let metadata = self.search_index.execute(|index| index.metadata())?;
                Ok(build_projection_lineage(
                    "proj:corpus",
                    &metadata.projection_watermark,
                    ObservationStats {
                        count: metadata.observation_count,
                        max_append_seq: metadata.last_append_seq,
                    },
                    usize::try_from(metadata.record_count).map_err(|_| {
                        SelfHostError::Ingestion(
                            "corpus record count does not fit usize".to_owned(),
                        )
                    })?,
                    metadata.committed_at,
                ))
            }
            "proj:answer-log" => Ok(build_projection_lineage(
                "proj:answer-log",
                &core.snapshot.lineage.build_id,
                core.observation_stats,
                core.snapshot.answer_log.len(),
                core.snapshot.built_at,
            )),
            "proj:claim-queue" => Ok(build_supplemental_projection_lineage(
                "proj:claim-queue",
                &core.supplemental.list(),
                core.snapshot.claim_queue.claims.len() + core.snapshot.claim_queue.decisions.len(),
                core.snapshot.built_at,
            )),
            "proj:freshness" => Ok(build_projection_lineage(
                "proj:freshness",
                &core.snapshot.lineage.build_id,
                core.observation_stats,
                core.snapshot.freshness.sources.len(),
                core.snapshot.built_at,
            )),
            "proj:reply-slo" => Ok(build_mixed_projection_lineage(
                "proj:reply-slo",
                &core.snapshot.lineage.build_id,
                core.observation_stats,
                &core.supplemental.list(),
                usize::try_from(core.reply_slo_count).map_err(|_| {
                    SelfHostError::Ingestion("reply SLO count does not fit usize".to_owned())
                })?,
                core.snapshot.built_at,
            )),
            "proj:break-glass" => Ok(build_channel_registry_projection_lineage(
                "proj:break-glass",
                &core.registry.list_channels(),
                core.snapshot.break_glass.channels.len(),
                core.snapshot.built_at,
            )),
            _ => Err(SelfHostError::NotFound(projection_id.to_string())),
        }
    }

    pub(super) fn apply_filter(
        &self,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, SelfHostError> {
        let result = FilteringGate::filter(&payload, AccessScope::Internal, &restricted_fields());
        self.emit_audit(
            "actor:self-host",
            AuditEventKind::ReadRestricted,
            serde_json::json!({
                "decision": "filtering-before-exposure",
                "masked_fields": result.masked_fields,
            }),
        )?;
        Ok(result.payload)
    }

    pub(super) fn resolve_read_mode(
        &self,
        catalog: &ProjectionCatalog,
        projection_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ReadMode, SelfHostError> {
        self.ensure_projection_fresh(catalog, projection_id)?;
        let spec = &catalog
            .get(&ProjectionRef::new(projection_id))
            .ok_or_else(|| SelfHostError::NotFound(projection_id.to_string()))?
            .spec;
        ReadModeResolver::resolve(spec, read_mode, pin)
            .map_err(|err: ReadModeError| SelfHostError::ReadMode(err.to_string()))
    }

    pub(super) fn ensure_projection_fresh(
        &self,
        catalog: &ProjectionCatalog,
        projection_id: &str,
    ) -> Result<(), SelfHostError> {
        let projection_ref = ProjectionRef::new(projection_id);
        let entry = catalog
            .get(&projection_ref)
            .ok_or_else(|| SelfHostError::NotFound(projection_id.to_owned()))?;
        if entry.status == ProjectionStatus::Stale || entry.health == ProjectionHealth::Stale {
            Err(SelfHostError::ProjectionStale(format!(
                "{projection_id} is stale"
            )))
        } else {
            Ok(())
        }
    }

    pub(super) fn refresh_materialized_snapshot(
        &self,
        core: &AppCore,
    ) -> Result<(), SelfHostError> {
        self.refresh_materialized_snapshot_with_reason(core, "recovery")
    }

    pub(super) fn refresh_materialized_snapshot_with_reason(
        &self,
        core: &AppCore,
        reason: &'static str,
    ) -> Result<(), SelfHostError> {
        if !matches!(reason, "migration" | "recovery" | "bootstrap") {
            return Err(SelfHostError::Ingestion(format!(
                "invalid background non-corpus rebuild reason {reason}"
            )));
        }
        let already_running = self
            .non_corpus_rebuild_in_flight
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err();
        if already_running {
            return Ok(());
        }
        *self
            .non_corpus_rebuild_error
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)? = None;
        #[cfg(test)]
        self.non_corpus_rebuild_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        self.non_corpus_rebuild_reasons
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?
            .push(reason);
        let service = self.clone();
        let freshness_thresholds = core.freshness_thresholds.clone();
        let channels = core
            .registry
            .list_channels()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let spawn_result = std::thread::Builder::new()
            .name("lethe-non-corpus-rebuild".to_owned())
            .spawn(move || {
                let result = service.run_background_materialized_rebuild(
                    &freshness_thresholds,
                    &channels,
                    reason,
                );
                if let Err(error) = result {
                    tracing::error!(error = %error, "background non-corpus materialization failed");
                    if let Ok(mut rebuild_error) = service.non_corpus_rebuild_error.lock() {
                        *rebuild_error = Some(error.to_string());
                    }
                    if let Err(mark_error) =
                        service.mark_live_core_non_corpus_materializations_stale()
                    {
                        tracing::error!(
                            error = %mark_error,
                            "failed to mark non-corpus materializations stale"
                        );
                    }
                }
                service
                    .non_corpus_rebuild_in_flight
                    .store(false, std::sync::atomic::Ordering::Release);
            });
        if let Err(error) = spawn_result {
            self.non_corpus_rebuild_in_flight
                .store(false, std::sync::atomic::Ordering::Release);
            tracing::error!(
                error = %error,
                "failed to spawn background non-corpus materialization"
            );
            return Err(SelfHostError::Ingestion(format!(
                "failed to spawn background non-corpus materialization: {error}"
            )));
        }
        Ok(())
    }

    pub(super) fn mark_live_core_non_corpus_materializations_stale(
        &self,
    ) -> Result<(), SelfHostError> {
        let _derived_lane = self
            .derived_projection_lane
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?;
        let mut core = self.core_lock()?;
        core.mark_non_corpus_materializations_stale();
        self.publish_core_snapshot(&core);
        Ok(())
    }

    fn run_background_materialized_rebuild(
        &self,
        freshness_thresholds: &[FreshnessThreshold],
        channels: &[lethe_registry::registry::ChannelRecord],
        reason: &'static str,
    ) -> Result<(), SelfHostError> {
        let _derived_lane = self
            .derived_projection_lane
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?;
        tracing::info!(
            import_timing = true,
            non_corpus_materialize_mode = "background",
            full_rebuild_reason = reason,
            "background non-corpus materialization started"
        );
        let storage = BackgroundRebuildStorage::new(self);
        let supplementals = storage.load_supplementals()?;
        let stats = storage.observation_stats()?;
        let materialized = rebuild_materialized_snapshot_paged(
            &storage,
            &supplementals,
            freshness_thresholds,
            channels,
            stats,
            self.config.corpus.rebuild_page_size,
            Utc::now(),
        )?;
        let mut core = self.core_lock()?;
        if core.observation_stats.max_append_seq > stats.max_append_seq
            || core.observation_stats.count > stats.count
        {
            return Err(SelfHostError::Ingestion(format!(
                "resident non-corpus watermark {}/{} advanced beyond background rebuild target {}/{} while derived lane was held",
                core.observation_stats.count,
                core.observation_stats.max_append_seq,
                stats.count,
                stats.max_append_seq
            )));
        }
        core.install_materialized(materialized);
        self.publish_core_snapshot(&core);
        drop(core);
        storage.set_state(
            "append_consumer:person-page",
            &stats.max_append_seq.to_string(),
        )?;
        let current_stats = storage.observation_stats()?;
        if current_stats.count < stats.count || current_stats.max_append_seq < stats.max_append_seq
        {
            return Err(SelfHostError::Ingestion(format!(
                "canonical watermark regressed from rebuild target {}/{} to {}/{}",
                stats.count,
                stats.max_append_seq,
                current_stats.count,
                current_stats.max_append_seq
            )));
        }
        if current_stats != stats {
            self.trigger_append_consumer();
        }
        storage.emit_completion(stats, current_stats);
        Ok(())
    }

    pub(super) fn wait_for_non_corpus_rebuild(&self) -> Result<(), SelfHostError> {
        for _ in 0..6000 {
            if !self
                .non_corpus_rebuild_in_flight
                .load(std::sync::atomic::Ordering::Acquire)
            {
                if let Some(error) = self
                    .non_corpus_rebuild_error
                    .lock()
                    .map_err(|_| SelfHostError::LockPoisoned)?
                    .as_ref()
                {
                    return Err(SelfHostError::Ingestion(format!(
                        "background non-corpus materialization failed: {error}"
                    )));
                }
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Err(SelfHostError::ProjectionStale(
            "background non-corpus materialization did not complete within 60 seconds".to_owned(),
        ))
    }

    fn materialize_after_observation_append_with_stats(
        &self,
        core: &mut AppCore,
        appended_observations: &[Observation],
        stats: ObservationStats,
        appended_fact_sequences: Option<&BTreeMap<String, u64>>,
    ) -> Result<(), SelfHostError> {
        (|| match classify_non_corpus_delta_with_reason(appended_observations).kind {
            NonCorpusDeltaKind::NoOp | NonCorpusDeltaKind::DeclaredSchemaSkip => {
                core.observation_stats = stats;
                Ok(())
            }
            NonCorpusDeltaKind::FreshnessOnly
            | NonCorpusDeltaKind::SlackMessage
            | NonCorpusDeltaKind::Communication => {
                let persistence = self.persistence_lock()?;
                let lookup = StorageComponentProjectionLookup {
                    storage: persistence.as_ref(),
                };
                let commit = match appended_fact_sequences {
                    Some(sequences) => apply_compact_incremental_delta_with_sequences(
                        core,
                        appended_observations,
                        stats,
                        Utc::now(),
                        sequences,
                        &lookup,
                    )?,
                    None => apply_compact_incremental_delta(
                        core,
                        appended_observations,
                        stats,
                        Utc::now(),
                        &lookup,
                    )?,
                };
                let manifest = core.manifest_value()?;
                persistence.commit_projection_items(
                    &ProjectionRef::new("proj:person-page"),
                    &manifest,
                    &commit,
                )?;
                let person_page_ref = ProjectionRef::new("proj:person-page");
                let has_published_snapshot =
                    core.catalog.get(&person_page_ref).is_some_and(|entry| {
                        entry.status == ProjectionStatus::Active
                            && entry.health == ProjectionHealth::Healthy
                    });
                let rebuild_in_flight = self
                    .non_corpus_rebuild_in_flight
                    .load(std::sync::atomic::Ordering::Acquire);
                let rebuild_failed = self
                    .non_corpus_rebuild_error
                    .lock()
                    .map_err(|_| SelfHostError::LockPoisoned)?
                    .is_some();
                if has_published_snapshot || (!rebuild_in_flight && !rebuild_failed) {
                    core.activate_non_corpus_projections();
                }
                Ok(())
            }
        })()
    }

    pub(super) fn ingest_draft(
        &self,
        draft: ObservationDraft,
    ) -> Result<IngestResult, SelfHostError> {
        let payload_bytes = serde_json::to_vec(&draft.payload)?.len();
        if payload_bytes > self.config.resource_limits.max_payload_bytes {
            return Err(SelfHostError::Ingestion(format!(
                "payload size {payload_bytes} exceeds configured maximum {}",
                self.config.resource_limits.max_payload_bytes
            )));
        }
        let observation = {
            let core = self.core_snapshot();
            match prepare_draft(&core, draft) {
                Ok(observation) => observation,
                Err(IngestResult::Rejected { message, .. }) => {
                    return Err(SelfHostError::Ingestion(message));
                }
                Err(IngestResult::Quarantined { ticket }) => {
                    return Err(SelfHostError::Ingestion(ticket.reason));
                }
                Err(result) => return Ok(result),
            }
        };

        let result = self.append_prepared_observation(observation)?;

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

    pub(super) fn append_prepared_observation(
        &self,
        observation: Observation,
    ) -> Result<IngestResult, SelfHostError> {
        let recorded_at = observation.recorded_at;

        let audit_event = self.build_audit_event(
            "actor:self-host",
            AuditEventKind::WriteExecution,
            serde_json::json!({"observation_id": observation.id.as_str()}),
        )?;
        let audit_record = AppService::audit_record(&audit_event)?;
        let source_instance_id = observation
            .meta
            .get("source_instance")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                SelfHostError::Ingestion(
                    "prepared observation is missing meta.source_instance".to_owned(),
                )
            })?;
        let durable_outcome = self
            .persistence_lock()?
            .append_observations_v1_with_admission(
                source_instance_id,
                None,
                std::slice::from_ref(&observation),
                std::slice::from_ref(&audit_record),
            )?
            .into_iter()
            .next()
            .ok_or_else(|| {
                SelfHostError::Ingestion("durable append returned no outcome".to_owned())
            })?;

        let result = match durable_outcome {
            DurableAppendOutcome::Appended(id) => IngestResult::Ingested { id, recorded_at },
            DurableAppendOutcome::Duplicate(existing_id) => IngestResult::Duplicate { existing_id },
            DurableAppendOutcome::CanonicalCollision(existing_id) => IngestResult::Quarantined {
                ticket: lethe_core::domain::QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    kind: lethe_core::domain::QuarantineKind::CanonicalCollision,
                    reason: format!(
                        "sha256-collision: existing observation {existing_id} has different canonical_json"
                    ),
                },
            },
        };
        Ok(result)
    }

    pub(super) fn append_prepared_slack_observation(
        &self,
        observation: Observation,
        thread: &SlackThreadKey,
    ) -> Result<IngestResult, SelfHostError> {
        let recorded_at = observation.recorded_at;
        let audit_event = self.build_audit_event(
            "actor:self-host",
            AuditEventKind::WriteExecution,
            serde_json::json!({"observation_id": observation.id.as_str()}),
        )?;
        let audit_record = AppService::audit_record(&audit_event)?;
        let source_instance_id = observation
            .meta
            .get("source_instance")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                SelfHostError::Ingestion(
                    "prepared Slack observation is missing meta.source_instance".to_owned(),
                )
            })?;
        let durable_outcome = self
            .persistence_lock()?
            .append_slack_observation_v1_with_admission(
                source_instance_id,
                None,
                &observation,
                thread,
                std::slice::from_ref(&audit_record),
            )?;
        let result = match durable_outcome {
            DurableAppendOutcome::Appended(id) => IngestResult::Ingested { id, recorded_at },
            DurableAppendOutcome::Duplicate(existing_id) => IngestResult::Duplicate { existing_id },
            DurableAppendOutcome::CanonicalCollision(existing_id) => IngestResult::Quarantined {
                ticket: lethe_core::domain::QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    kind: lethe_core::domain::QuarantineKind::CanonicalCollision,
                    reason: format!(
                        "sha256-collision: existing observation {existing_id} has different canonical_json"
                    ),
                },
            },
        };
        Ok(result)
    }

    pub(super) fn store_blob(&self, data: &[u8]) -> Result<BlobRef, SelfHostError> {
        let blob_ref = self
            .persistence_lock()?
            .put_blob(data, self.config.resource_limits.max_blob_bytes)?;
        let mut core = self.core_lock()?;
        core.blobs.put(data);
        self.publish_core_snapshot(&core);
        Ok(blob_ref)
    }

    pub fn projection_blob_bytes(
        &self,
        projection: &ProjectionRef,
        blob_ref: &BlobRef,
    ) -> Result<Option<Vec<u8>>, SelfHostError> {
        let core = self.core_snapshot();
        self.ensure_projection_fresh(&core.catalog, projection.as_str())?;
        drop(core);
        let referenced = self
            .persistence_read_lock()?
            .projection_blob_ref_visible(projection, blob_ref)?;
        self.emit_audit(
            "actor:self-host",
            AuditEventKind::BlobAuthorization,
            serde_json::json!({
                "actor": "actor:self-host",
                "subject": format!("blob:{blob_ref}"),
                "scope": format!("projection:{}", projection.as_str()),
                "decision": if referenced { "visible" } else { "deny" },
                "rule": "projection_visible_blob_refs subject-key lookup",
                "timestamp": Utc::now(),
                "exposure_rule": "filtering-before-exposure",
                "masked_fields": [],
            }),
        )?;
        if !referenced {
            return Ok(None);
        }
        Ok(self.persistence_read_lock()?.get_blob(blob_ref)?)
    }

    /// Run the explicit operator-triggered privacy completeness validation.
    /// Normal append/consumer paths use incremental folds and do not call
    /// this method.
    pub fn validate_privacy_projections_on_demand(
        &self,
    ) -> Result<lethe_projection_corpus::PrivacyValidationReport, SelfHostError> {
        let observations = self.persistence_read_lock()?.load_observations()?;
        let projector = CorpusProjector::new(self.config.corpus.projector_config());
        let report = projector
            .validate_on_demand_full(&observations)
            .map_err(SelfHostError::Ingestion)?;
        self.search_index.execute(|index| index.validate())?;
        Ok(report)
    }

    pub(super) fn ingest_slack_message<A: SlackClient, F: SlackClient>(
        &self,
        slack_adapter: &SlackAdapter<A>,
        file_client: &F,
        source_instance_id: &str,
        channel_id: &str,
        mut message: lethe_adapter_slack::slack::client::SlackMessage,
        latest_ts: &mut Option<String>,
    ) -> Result<IngestResult, SelfHostError> {
        message.channel_id = channel_id.to_string();
        let source = self
            .config
            .slack_sources
            .iter()
            .find(|source| source.id == source_instance_id)
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "slack source instance {source_instance_id} is not configured"
                ))
            })?;
        message.ingress_kind = Some(classify_slack_ingress(
            channel_id,
            &message.mentions,
            &source.mention_user_ids,
        ));
        for file in &mut message.files {
            if file.blob_ref.is_none() {
                let policy = self.slack_adapter_config();
                let data = self.resilient_executor.execute(
                    &format!("slack:{source_instance_id}:file-download"),
                    &policy.retry,
                    &policy.rate_limit,
                    || file_client.file_download(file),
                )?;
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
        let thread = thread_root_ts(&message).map(|thread_ts| SlackThreadKey {
            source_instance: source_instance_id.to_owned(),
            channel_id: channel_id.to_owned(),
            thread_ts: thread_ts.to_owned(),
        });
        let draft = namespace_draft(slack_adapter.map_message(&message)?, source_instance_id);
        let Some(thread) = thread else {
            return self.ingest_draft(draft);
        };
        let payload_bytes = serde_json::to_vec(&draft.payload)?.len();
        if payload_bytes > self.config.resource_limits.max_payload_bytes {
            return Err(SelfHostError::Ingestion(format!(
                "payload size {payload_bytes} exceeds configured maximum {}",
                self.config.resource_limits.max_payload_bytes
            )));
        }
        let observation = {
            let core = self.core_snapshot();
            match prepare_draft(&core, draft) {
                Ok(observation) => observation,
                Err(IngestResult::Rejected { message, .. }) => {
                    return Err(SelfHostError::Ingestion(message));
                }
                Err(IngestResult::Quarantined { ticket }) => {
                    return Err(SelfHostError::Ingestion(ticket.reason));
                }
                Err(result) => return Ok(result),
            }
        };
        self.append_prepared_slack_observation(observation, &thread)
    }

    pub(super) fn sync_thread_replies<A: SlackClient, F: SlackClient, R: SlackClient>(
        &self,
        slack_adapter: &SlackAdapter<A>,
        file_client: &F,
        replies_client: &R,
        thread: &SlackThreadCatalogEntry,
        generation: u64,
    ) -> Result<(usize, usize, usize), SelfHostError> {
        let key = &thread.key;
        let policy = self.slack_adapter_config();
        let replies = self.resilient_executor.execute(
            &format!("slack:{}:{}:replies", key.source_instance, key.channel_id),
            &policy.retry,
            &policy.rate_limit,
            || {
                replies_client.conversations_replies(
                    &key.channel_id,
                    &key.thread_ts,
                    Some(thread.reply_cursor.as_str()),
                )
            },
        )?;
        let cursor_value = slack_ts_value(&thread.reply_cursor)?;
        let mut latest_reply_ts = Some(thread.reply_cursor.clone());
        let mut ingested = 0usize;
        let mut duplicates = 0usize;
        let mut fetched = 0usize;

        for reply in replies
            .into_iter()
            .filter(|reply| reply.ts != key.thread_ts)
        {
            if slack_ts_value(&reply.ts)? <= cursor_value {
                continue;
            }
            if reply.thread_ts.as_deref() != Some(key.thread_ts.as_str()) {
                return Err(SelfHostError::Ingestion(format!(
                    "Slack thread {} reply {} has mismatched thread_ts",
                    key.thread_ts, reply.ts
                )));
            }
            fetched += 1;
            match self.ingest_slack_message(
                slack_adapter,
                file_client,
                &key.source_instance,
                &key.channel_id,
                reply,
                &mut latest_reply_ts,
            )? {
                IngestResult::Ingested { .. } => ingested += 1,
                IngestResult::Duplicate { .. } => duplicates += 1,
                _ => {}
            }
        }

        let latest_reply_ts = latest_reply_ts.expect("thread reply cursor is always initialized");
        let delay = if fetched > 0 {
            1
        } else {
            IDLE_THREAD_RECHECK_INTERVAL
        };
        let next_poll_generation = generation.checked_add(delay).ok_or_else(|| {
            SelfHostError::Ingestion("Slack thread next poll generation overflowed u64".to_owned())
        })?;
        self.persistence_lock()?.complete_slack_thread_poll(
            key,
            generation,
            &latest_reply_ts,
            fetched > 0,
            next_poll_generation,
        )?;

        Ok((ingested, duplicates, fetched))
    }

    pub(super) fn refresh_slack_thread_catalog(&self) -> Result<(), SelfHostError> {
        let mut cursor = self
            .persistence_lock()?
            .slack_thread_discovery_high_water()?;
        loop {
            let page = self.persistence_lock()?.observation_page(cursor, 512)?;
            if page.is_empty() {
                return Ok(());
            }
            let high_water = page
                .last()
                .expect("non-empty observation discovery page must have a tail")
                .append_seq;
            let threads = discovered_slack_threads(&page)?;
            self.persistence_lock()?
                .commit_slack_thread_discovery(high_water, &threads)?;
            cursor = high_water;
        }
    }

    pub(super) fn extract_student_profile_from_png(
        &self,
        image: &[u8],
        observation: &Observation,
        canonical_uri: &str,
    ) -> Result<Option<StudentProfile>, SelfHostError> {
        let title = observation
            .payload
            .get("title")
            .and_then(|value| value.as_str())
            .unwrap_or("Unknown");

        let policy = self.google_adapter_config();
        let analyzer = self.slide_analyzer()?;
        Ok(self.resilient_executor.execute(
            "derivation:gemini",
            &policy.retry,
            &policy.rate_limit,
            || analyzer.extract_profile_from_png(image, title, canonical_uri),
        )?)
    }

    pub(super) fn slide_analyzer(&self) -> Result<&GeminiSlideAnalyzer, SelfHostError> {
        self.slide_analyzer.as_ref().ok_or_else(|| {
            SelfHostError::Config(crate::self_host::config::ConfigError::Invalid(
                "slide analyzer is not configured".to_owned(),
            ))
        })
    }

    pub(super) fn core_lock(&self) -> Result<AppCoreWriteGuard<'_>, SelfHostError> {
        let guard = self.core.lock().map_err(|_| SelfHostError::LockPoisoned)?;
        Ok(AppCoreWriteGuard { guard })
    }

    pub(super) fn core_snapshot(&self) -> Arc<AppCore> {
        self.core_snapshot.load_full()
    }

    pub(super) fn publish_core_snapshot_for_import(
        &self,
        core: &AppCore,
        timer: &mut super::ObservationImportTimer,
    ) {
        let started_at = std::time::Instant::now();
        self.publish_core_snapshot(core);
        timer.record_stage(super::ImportTimingStage::PublishClone, started_at.elapsed());
    }

    pub(super) fn publish_core_snapshot(&self, core: &AppCore) {
        let old_snapshot = self.core_snapshot.load_full();
        tracing::debug!(
            old_arc_strong_count = std::sync::Arc::strong_count(&old_snapshot),
            "publishing AppCore snapshot"
        );
        #[cfg(test)]
        self.publish_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.core_snapshot.store(Arc::new(core.clone()));
    }

    pub(super) fn try_acquire_import_permit(&self) -> Result<ImportPermit, SelfHostError> {
        let maximum = self.config.resource_limits.max_concurrent_imports;
        let mut current = self
            .import_in_flight
            .load(std::sync::atomic::Ordering::Acquire);
        loop {
            if current >= maximum {
                return Err(SelfHostError::ImportConcurrencyLimit { maximum });
            }
            match self.import_in_flight.compare_exchange_weak(
                current,
                current + 1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ImportPermit {
                        in_flight: Arc::clone(&self.import_in_flight),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    #[cfg(test)]
    pub(super) fn publish_count(&self) -> usize {
        self.publish_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(super) fn persistence_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Box<dyn StoragePorts>>, SelfHostError> {
        self.persistence
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)
    }

    pub(super) fn persistence_lock_for_import<'a>(
        &'a self,
        timer: &mut ObservationImportTimer,
    ) -> Result<std::sync::MutexGuard<'a, Box<dyn StoragePorts>>, SelfHostError> {
        let wait_started_at = Instant::now();
        let result = self
            .persistence
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned);
        timer.record_stage(
            ImportTimingStage::PersistenceLockWait,
            wait_started_at.elapsed(),
        );
        result
    }

    pub(super) fn persistence_read_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Box<dyn StoragePorts>>, SelfHostError> {
        let index = self
            .persistence_read_next
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.persistence_read_pool.len();
        self.persistence_read_pool[index]
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)
    }

    pub(super) fn operational_ledger_read_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Box<dyn OperationalStoragePorts>>, SelfHostError> {
        let index = self
            .operational_ledger_read_next
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.operational_ledger_read_pool.len();
        self.operational_ledger_read_pool[index]
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)
    }

    fn append_consumer_health_dependency(&self) -> Result<DependencyHealthInfo, SelfHostError> {
        let error = self
            .append_consumer_error
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?
            .clone();
        Ok(DependencyHealthInfo {
            name: "append_seq_consumer".to_owned(),
            status: if error.is_some() { "failed" } else { "ok" }.to_owned(),
            detail: error,
        })
    }

    pub(super) fn trigger_append_consumer(&self) {
        if self
            .append_consumer_in_flight
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let service = self.clone();
        std::thread::spawn(move || {
            let result = service.run_append_consumer();
            if let Err(error) = result {
                tracing::error!(error = %error, "append-seq consumer failed");
                if let Err(mark_error) = service.mark_live_core_non_corpus_materializations_stale()
                {
                    tracing::error!(
                        error = %mark_error,
                        "failed to mark non-corpus materializations stale"
                    );
                }
                if let Ok(mut state) = service.append_consumer_error.lock() {
                    *state = Some(error.to_string());
                }
                if let Err(audit_error) = service.emit_audit(
                    "actor:append-seq-consumer",
                    AuditEventKind::Rejection,
                    serde_json::json!({
                        "projection": "proj:person-page",
                        "error": error.to_string(),
                        "kind": "projection_consumer_failure",
                    }),
                ) {
                    tracing::error!(
                        error = %audit_error,
                        "failed to durably record append-seq consumer failure"
                    );
                    if let Ok(mut state) = service.append_consumer_error.lock() {
                        *state = Some(format!("{error}; audit error: {audit_error}"));
                    }
                }
            } else if let Ok(mut state) = service.append_consumer_error.lock() {
                *state = None;
            }
            service
                .append_consumer_in_flight
                .store(false, std::sync::atomic::Ordering::Release);
        });
    }

    pub(super) fn trigger_search_index_catch_up(&self) {
        if self
            .search_index_catch_up_in_flight
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let search_index = self.search_index.clone();
        let in_flight = Arc::clone(&self.search_index_catch_up_in_flight);
        let spawn_result = std::thread::Builder::new()
            .name("lethe-search-index-catch-up".to_owned())
            .spawn(move || {
                if let Err(error) = search_index.catch_up_after_append() {
                    tracing::error!(error = %error, "search index catch-up failed after canonical append");
                }
                in_flight.store(false, std::sync::atomic::Ordering::Release);
            });
        if let Err(error) = spawn_result {
            self.search_index_catch_up_in_flight
                .store(false, std::sync::atomic::Ordering::Release);
            tracing::error!(error = %error, "failed to spawn search index catch-up");
        }
    }

    fn run_append_consumer(&self) -> Result<(), SelfHostError> {
        let _derived_lane = self
            .derived_projection_lane
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?;
        loop {
            self.catch_up_identity_bridge()?;
            let cursor = self
                .persistence_read_lock()?
                .get_state("append_consumer:person-page")?
                .ok_or_else(|| {
                    SelfHostError::Ingestion(
                        "append consumer cursor is missing from persistent state".to_owned(),
                    )
                })?
                .parse::<u64>()
                .map_err(|_| {
                    SelfHostError::Ingestion("append consumer cursor is not a valid u64".to_owned())
                })?;
            let pending = self
                .persistence_read_lock()?
                .observation_stats()?
                .max_append_seq
                .saturating_sub(cursor);
            let page_limit = usize::try_from(pending.min(16_384)).map_err(|_| {
                SelfHostError::Ingestion(
                    "append consumer pending count does not fit usize".to_owned(),
                )
            })?;
            let page = self
                .persistence_read_lock()?
                .observation_page(cursor, page_limit.max(1))?;
            if page.is_empty() {
                self.trigger_search_index_catch_up();
                return Ok(());
            }
            let observations = page
                .iter()
                .map(|stored| stored.observation.clone())
                .collect::<Vec<_>>();
            let next_cursor = page.last().map(|stored| stored.append_seq).ok_or_else(|| {
                SelfHostError::Ingestion("append consumer received an empty page".to_owned())
            })?;
            let base_count = self.core_snapshot().observation_stats.count;
            let batch_count = base_count
                .checked_add(u64::try_from(observations.len()).map_err(|_| {
                    SelfHostError::Ingestion(
                        "append consumer page size does not fit u64".to_owned(),
                    )
                })?)
                .ok_or_else(|| {
                    SelfHostError::Ingestion(
                        "append consumer observation count overflowed u64".to_owned(),
                    )
                })?;
            let mut core = (*self.core_snapshot()).clone();
            let appended_fact_sequences = page
                .iter()
                .filter(|stored| {
                    stored.observation.schema.as_str() == "schema:slack-message"
                        || identity_replay_event(&stored.observation, 1).is_some()
                })
                .map(|stored| (stored.observation.id.as_str().to_owned(), stored.append_seq))
                .collect::<BTreeMap<_, _>>();
            self.materialize_after_observation_append_with_stats(
                &mut core,
                &observations,
                ObservationStats {
                    count: batch_count,
                    max_append_seq: next_cursor,
                },
                Some(&appended_fact_sequences),
            )?;
            let mut live_core = self.core_lock()?;
            *live_core = core;
            self.publish_core_snapshot(&live_core);
            self.persistence_lock()?
                .set_state("append_consumer:person-page", &next_cursor.to_string())?;
        }
    }

    pub(super) fn slack_adapter_config(&self) -> AdapterConfig {
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

    pub(super) fn google_adapter_config(&self) -> AdapterConfig {
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

pub(super) struct AppCoreWriteGuard<'a> {
    guard: std::sync::MutexGuard<'a, AppCore>,
}

impl std::ops::Deref for AppCoreWriteGuard<'_> {
    type Target = AppCore;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for AppCoreWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

pub(super) fn classify_slack_ingress(
    channel_id: &str,
    mentions: &[String],
    mention_user_ids: &[String],
) -> lethe_adapter_slack::slack::client::SlackIngressKind {
    if channel_id.starts_with('D') {
        return lethe_adapter_slack::slack::client::SlackIngressKind::DirectMessage;
    }
    if mentions.iter().any(|mention| {
        mention_user_ids
            .iter()
            .any(|candidate| candidate == mention)
    }) {
        return lethe_adapter_slack::slack::client::SlackIngressKind::Mention;
    }
    lethe_adapter_slack::slack::client::SlackIngressKind::Channel
}

pub(super) fn namespace_draft(
    mut draft: ObservationDraft,
    source_instance_id: &str,
) -> ObservationDraft {
    draft.idempotency_key = lethe_core::domain::IdempotencyKey::new(format!(
        "{source_instance_id}:{}",
        draft.idempotency_key.as_str()
    ));
    let mut meta = draft.meta.as_object().cloned().unwrap_or_default();
    let container = meta
        .get("source_container")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("root")
        .to_owned();
    meta.insert(
        "source_instance".to_owned(),
        serde_json::Value::String(source_instance_id.to_owned()),
    );
    meta.insert(
        "source_container".to_owned(),
        serde_json::Value::String(format!("{source_instance_id}:{container}")),
    );
    draft.meta = serde_json::Value::Object(meta);
    draft
}

pub(super) fn build_person_page_lineage(
    canonical_observation_fingerprint: &str,
    stats: ObservationStats,
    supplemental_fingerprint: &str,
    supplemental_count: usize,
    output_count: usize,
    built_at: DateTime<Utc>,
) -> LineageManifest {
    let build_id = person_page_build_id(
        canonical_observation_fingerprint,
        stats.count,
        supplemental_fingerprint,
    );
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
        watermark_position: Some(
            usize::try_from(stats.max_append_seq)
                .expect("canonical append sequence must fit usize"),
        ),
        record_count: usize::try_from(stats.count)
            .expect("canonical observation count must fit usize"),
    });
    lineage.add_source(SourceSnapshot {
        source_ref: "supplemental".to_string(),
        watermark_position: None,
        record_count: supplemental_count,
    });
    lineage
}

pub(super) fn build_projection_lineage(
    projection_id: &str,
    canonical_build_id: &str,
    stats: ObservationStats,
    output_count: usize,
    built_at: DateTime<Utc>,
) -> LineageManifest {
    let mut hasher = Sha256::new();
    hasher.update(projection_id.as_bytes());
    hasher.update(b"@1.0.0\n");
    hasher.update(canonical_build_id.as_bytes());
    hasher.update(b"\n");
    hasher.update(stats.count.to_be_bytes());
    hasher.update(stats.max_append_seq.to_be_bytes());
    let build_id = format!("build-{}", hex::encode(hasher.finalize()));
    let mut lineage = LineageManifest::new(
        ProjectionRef::new(projection_id),
        SemVer::new("1.0.0"),
        build_id,
    );
    lineage.built_at = built_at;
    lineage.output_count = output_count;
    lineage.deterministic = true;
    lineage.add_source(SourceSnapshot {
        source_ref: "lake".to_string(),
        watermark_position: Some(
            usize::try_from(stats.max_append_seq)
                .expect("canonical append sequence must fit usize"),
        ),
        record_count: usize::try_from(stats.count)
            .expect("canonical observation count must fit usize"),
    });
    lineage
}

pub(super) fn build_supplemental_projection_lineage(
    projection_id: &str,
    supplementals: &[&lethe_core::domain::SupplementalRecord],
    output_count: usize,
    built_at: DateTime<Utc>,
) -> LineageManifest {
    let mut input_refs = supplementals
        .iter()
        .map(|record| format!("supplemental:{}", record.id))
        .collect::<Vec<_>>();
    input_refs.sort();

    let mut hasher = Sha256::new();
    hasher.update(projection_id.as_bytes());
    hasher.update(b"@1.0.0\n");
    for input_ref in &input_refs {
        hasher.update(input_ref.as_bytes());
        hasher.update(b"\n");
    }
    let build_id = format!("build-{}", hex::encode(hasher.finalize()));
    let mut lineage = LineageManifest::new(
        ProjectionRef::new(projection_id),
        SemVer::new("1.0.0"),
        build_id,
    );
    lineage.built_at = built_at;
    lineage.output_count = output_count;
    lineage.deterministic = true;
    lineage.add_source(SourceSnapshot {
        source_ref: "supplemental".to_string(),
        watermark_position: None,
        record_count: supplementals.len(),
    });
    for input_ref in input_refs {
        lineage.add_input_ref(input_ref);
    }
    lineage
}

pub(super) fn build_mixed_projection_lineage(
    projection_id: &str,
    canonical_build_id: &str,
    stats: ObservationStats,
    supplementals: &[&lethe_core::domain::SupplementalRecord],
    output_count: usize,
    built_at: DateTime<Utc>,
) -> LineageManifest {
    let mut input_refs = supplementals
        .iter()
        .map(|record| format!("supplemental:{}", record.id))
        .collect::<Vec<_>>();
    input_refs.sort();

    let mut hasher = Sha256::new();
    hasher.update(projection_id.as_bytes());
    hasher.update(b"@1.0.0\n");
    hasher.update(canonical_build_id.as_bytes());
    hasher.update(b"\n");
    hasher.update(stats.count.to_be_bytes());
    hasher.update(stats.max_append_seq.to_be_bytes());
    for input_ref in &input_refs {
        hasher.update(input_ref.as_bytes());
        hasher.update(b"\n");
    }
    let build_id = format!("build-{}", hex::encode(hasher.finalize()));
    let mut lineage = LineageManifest::new(
        ProjectionRef::new(projection_id),
        SemVer::new("1.0.0"),
        build_id,
    );
    lineage.built_at = built_at;
    lineage.output_count = output_count;
    lineage.deterministic = true;
    lineage.add_source(SourceSnapshot {
        source_ref: "lake".to_string(),
        watermark_position: Some(
            usize::try_from(stats.max_append_seq)
                .expect("canonical append sequence must fit usize"),
        ),
        record_count: usize::try_from(stats.count)
            .expect("canonical observation count must fit usize"),
    });
    lineage.add_source(SourceSnapshot {
        source_ref: "supplemental".to_string(),
        watermark_position: None,
        record_count: supplementals.len(),
    });
    for input_ref in input_refs {
        lineage.add_input_ref(input_ref);
    }
    lineage
}

pub(super) fn build_channel_registry_projection_lineage(
    projection_id: &str,
    channels: &[&lethe_registry::registry::ChannelRecord],
    output_count: usize,
    built_at: DateTime<Utc>,
) -> LineageManifest {
    let mut input_refs = channels
        .iter()
        .map(|channel| format!("channel:{}", channel.id))
        .collect::<Vec<_>>();
    input_refs.sort();

    let mut hasher = Sha256::new();
    hasher.update(projection_id.as_bytes());
    hasher.update(b"@1.0.0\n");
    for input_ref in &input_refs {
        hasher.update(input_ref.as_bytes());
        hasher.update(b"\n");
    }
    let build_id = format!("build-{}", hex::encode(hasher.finalize()));
    let mut lineage = LineageManifest::new(
        ProjectionRef::new(projection_id),
        SemVer::new("1.0.0"),
        build_id,
    );
    lineage.built_at = built_at;
    lineage.output_count = output_count;
    lineage.deterministic = true;
    lineage.add_source(SourceSnapshot {
        source_ref: "registry:channels".to_string(),
        watermark_position: None,
        record_count: channels.len(),
    });
    for input_ref in input_refs {
        lineage.add_input_ref(input_ref);
    }
    lineage
}

pub(super) fn consent_status_for_person_id(
    core: &AppCore,
    person_id: &str,
) -> Result<ConsentStatus, SelfHostError> {
    core.person_consents
        .get(person_id)
        .cloned()
        .ok_or_else(|| SelfHostError::NotFound(person_id.to_string()))
}

pub(super) fn lineage_ref(lineage: &LineageManifest) -> String {
    format!(
        "lineage:{}:{}",
        lineage.projection_id.as_str().trim_start_matches("proj:"),
        lineage.build_id
    )
}
