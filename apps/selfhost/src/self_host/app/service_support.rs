use super::*;

impl AppService {
    pub fn mcp_oauth_config(&self) -> crate::self_host::config::McpOAuthConfig {
        self.config.mcp_oauth.clone()
    }

    pub fn health(&self) -> Result<HealthResponse, SelfHostError> {
        let core = self.core_lock()?;
        Ok(
            HealthResponse::from_catalog(&core.catalog, env!("CARGO_PKG_VERSION")).with_runtime(
                vec![
                    self.bulk_import_health_dependency()?,
                    self.search_index.health_dependency(),
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
        let core = self.core_lock()?;
        Ok(
            HealthResponse::from_catalog(&core.catalog, env!("CARGO_PKG_VERSION")).with_runtime(
                vec![
                    storage_dependency,
                    self.bulk_import_health_dependency()?,
                    self.search_index.deep_health_dependency(),
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
        let core = self.core_lock()?;
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

    pub(super) fn apply_filter(&self, payload: serde_json::Value) -> serde_json::Value {
        let result = FilteringGate::filter(&payload, AccessScope::Internal, &restricted_fields());
        self.emit_audit(
            "actor:self-host",
            AuditEventKind::ReadRestricted,
            serde_json::json!({
                "decision": "filtering-before-exposure",
                "masked_fields": result.masked_fields,
            }),
        );
        result.payload
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
        core: &mut AppCore,
    ) -> Result<(), SelfHostError> {
        self.refresh_materialized_snapshot_with_reason(core, "recovery")
    }

    pub(super) fn refresh_materialized_snapshot_with_reason(
        &self,
        core: &mut AppCore,
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
                    if let Ok(mut core) = service.core_lock() {
                        core.mark_non_corpus_materializations_stale();
                    }
                }
                service
                    .non_corpus_rebuild_in_flight
                    .store(false, std::sync::atomic::Ordering::Release);
            });
        if let Err(error) = spawn_result {
            self.non_corpus_rebuild_in_flight
                .store(false, std::sync::atomic::Ordering::Release);
            core.mark_non_corpus_materializations_stale();
            return Err(SelfHostError::Ingestion(format!(
                "failed to spawn background non-corpus materialization: {error}"
            )));
        }
        Ok(())
    }

    fn run_background_materialized_rebuild(
        &self,
        freshness_thresholds: &[FreshnessThreshold],
        channels: &[lethe_registry::registry::ChannelRecord],
        reason: &'static str,
    ) -> Result<(), SelfHostError> {
        tracing::info!(
            import_timing = true,
            non_corpus_materialize_mode = "background",
            full_rebuild_reason = reason,
            "background non-corpus materialization started"
        );
        loop {
            let (materialized, stats) = {
                let store = self.persistence_lock()?;
                let supplementals = store.load_supplementals()?;
                let stats = store.observation_stats()?;
                let materialized = rebuild_materialized_snapshot_paged(
                    store.as_ref(),
                    &supplementals,
                    freshness_thresholds,
                    channels,
                    stats,
                    self.config.corpus.rebuild_page_size,
                    Utc::now(),
                )?;
                (materialized, stats)
            };
            let current_stats = self.persistence_lock()?.observation_stats()?;
            if current_stats != stats {
                tracing::debug!(
                    built_count = stats.count,
                    current_count = current_stats.count,
                    "background non-corpus rebuild observed a newer canonical high-water; retrying"
                );
                continue;
            }
            let mut core = self.core_lock()?;
            if core.observation_stats.max_append_seq > stats.max_append_seq
                || core.observation_stats.count > stats.count
            {
                continue;
            }
            core.install_materialized(materialized);
            return Ok(());
        }
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

    pub(super) fn materialize_after_observation_append(
        &self,
        core: &mut AppCore,
        appended_observations: &[Observation],
    ) -> Result<(), SelfHostError> {
        let result = (|| match classify_non_corpus_delta_with_reason(appended_observations).kind {
            NonCorpusDeltaKind::NoOp | NonCorpusDeltaKind::DeclaredSchemaSkip => Ok(()),
            NonCorpusDeltaKind::FreshnessOnly
            | NonCorpusDeltaKind::SlackMessage
            | NonCorpusDeltaKind::Communication => {
                let declared_observations = appended_observations
                    .iter()
                    .filter(|observation| {
                        projection_fold_behavior(observation.schema.as_str()).is_some()
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if declared_observations.is_empty() {
                    return Ok(());
                }
                let persistence = self.persistence_lock()?;
                let stats = persistence.observation_stats()?;
                let lookup = StorageComponentProjectionLookup {
                    storage: persistence.as_ref(),
                };
                let commit = apply_compact_incremental_delta(
                    core,
                    &declared_observations,
                    stats,
                    Utc::now(),
                    &lookup,
                )?;
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
        })();
        if result.is_err() {
            core.mark_non_corpus_materializations_stale();
        }
        result
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
            let core = self.core_lock()?;
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

        let durable_outcome = self.persistence_lock()?.append_observation(&observation)?;

        let result = match durable_outcome {
            DurableAppendOutcome::Appended(id) => {
                self.emit_audit(
                    "actor:self-host",
                    AuditEventKind::WriteExecution,
                    serde_json::json!({"observation_id": id.as_str()}),
                );
                IngestResult::Ingested { id, recorded_at }
            }
            DurableAppendOutcome::Duplicate(existing_id) => IngestResult::Duplicate { existing_id },
            DurableAppendOutcome::CanonicalCollision(existing_id) => IngestResult::Quarantined {
                ticket: lethe_core::domain::QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
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
        let durable_outcome = self
            .persistence_lock()?
            .append_slack_observation(&observation, thread)?;
        let result = match durable_outcome {
            DurableAppendOutcome::Appended(id) => {
                self.emit_audit(
                    "actor:self-host",
                    AuditEventKind::WriteExecution,
                    serde_json::json!({"observation_id": id.as_str()}),
                );
                IngestResult::Ingested { id, recorded_at }
            }
            DurableAppendOutcome::Duplicate(existing_id) => IngestResult::Duplicate { existing_id },
            DurableAppendOutcome::CanonicalCollision(existing_id) => IngestResult::Quarantined {
                ticket: lethe_core::domain::QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    reason: format!(
                        "sha256-collision: existing observation {existing_id} has different canonical_json"
                    ),
                },
            },
        };
        Ok(result)
    }

    pub(super) fn store_blob(&self, data: &[u8]) -> Result<BlobRef, SelfHostError> {
        let mut core = self.core_lock()?;
        let blob_ref = self
            .persistence_lock()?
            .put_blob(data, self.config.resource_limits.max_blob_bytes)?;
        core.blobs.put(data);
        Ok(blob_ref)
    }

    pub fn projection_blob_bytes(
        &self,
        blob_ref: &BlobRef,
    ) -> Result<Option<Vec<u8>>, SelfHostError> {
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:person-page")?;
        let referenced = core.person_components.values().any(|component| {
            component.consent != ConsentStatus::OptedOut
                && (component.slide_blob_refs.contains(blob_ref.as_str())
                    || component
                        .frontend_profile
                        .as_ref()
                        .and_then(|profile| profile.thumbnail_ref.as_deref())
                        == Some(blob_ref.as_str()))
        });
        drop(core);
        self.emit_audit(
            "actor:self-host",
            AuditEventKind::ReadRestricted,
            serde_json::json!({
                "decision": "filtering-before-exposure",
                "masked_fields": [],
            }),
        );
        if !referenced {
            return Ok(None);
        }
        Ok(self.persistence_lock()?.get_blob(blob_ref)?)
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
            let core = self.core_lock()?;
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

    pub(super) fn core_lock(&self) -> Result<std::sync::MutexGuard<'_, AppCore>, SelfHostError> {
        self.core.lock().map_err(|_| SelfHostError::LockPoisoned)
    }

    pub(super) fn persistence_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Box<dyn StoragePorts>>, SelfHostError> {
        self.persistence
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)
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
