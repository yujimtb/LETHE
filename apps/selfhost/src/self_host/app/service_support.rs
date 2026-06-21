use super::*;

impl AppService {
    pub fn health(&self) -> Result<HealthResponse, SelfHostError> {
        let core = self.core_lock()?;
        Ok(HealthResponse::from_catalog(
            &core.catalog,
            env!("CARGO_PKG_VERSION"),
        ))
    }

    pub(super) fn authorize_read(&self, target: EntityRef) -> Result<(), SelfHostError> {
        let outcome = PolicyEngine::evaluate(&PolicyRequest {
            actor: ActorRef::new("actor:self-host"),
            role: Role::Researcher,
            operation: Operation::Read { target },
            data_scope: AccessScope::Internal,
            consent_status: ConsentStatus::Unrestricted,
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
            lineage_ref: None,
        })
    }

    pub(super) fn apply_filter(&self, payload: serde_json::Value) -> serde_json::Value {
        FilteringGate::filter(&payload, AccessScope::Internal, &restricted_fields()).payload
    }

    pub(super) fn resolve_read_mode(
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

    pub(super) fn ingest_draft(
        &self,
        draft: ObservationDraft,
    ) -> Result<IngestResult, SelfHostError> {
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

    pub(super) fn append_prepared_observation(
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
                lethe_engine::lake::store::AppendOutcome::Appended(_) => {
                    IngestResult::Ingested { id, recorded_at }
                }
                lethe_engine::lake::store::AppendOutcome::Duplicate(existing_id)
                | lethe_engine::lake::store::AppendOutcome::Conflict(existing_id) => {
                    return Err(SelfHostError::Ingestion(format!(
                        "SQLite accepted observation {id}, but cache already contains {existing_id}"
                    )));
                }
            },
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
        let blob_ref = core.blobs.put(data);
        self.persistence_lock()?.persist_blob(data)?;
        Ok(blob_ref)
    }

    pub fn blob_bytes(&self, blob_ref: &BlobRef) -> Result<Option<Vec<u8>>, SelfHostError> {
        let core = self.core_lock()?;
        Ok(core.blobs.get(blob_ref).map(|bytes| bytes.to_vec()))
    }

    pub(super) fn ingest_slack_message(
        &self,
        slack_adapter: &SlackAdapter<HttpSlackClient>,
        file_client: &HttpSlackClient,
        channel_id: &str,
        mut message: lethe_adapter_slack::slack::client::SlackMessage,
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
        if latest_ts
            .as_ref()
            .map(|current| slack_ts_value(&message.ts) > slack_ts_value(current))
            .unwrap_or(true)
        {
            *latest_ts = Some(message.ts.clone());
        }
        self.ingest_draft(slack_adapter.map_message(&message))
    }

    pub(super) fn sync_thread_replies(
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

    pub(super) fn known_thread_roots(
        &self,
        channel_id: &str,
    ) -> Result<BTreeSet<String>, SelfHostError> {
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

    pub(super) fn extract_student_profile_from_png(
        &self,
        image: &[u8],
        observation: &Observation,
        canonical_uri: &str,
    ) -> Option<StudentProfile> {
        let title = observation
            .payload
            .get("title")
            .and_then(|value| value.as_str())
            .unwrap_or("Unknown");

        if let Some(analyzer) = &self.slide_analyzer {
            match analyzer.extract_profile_from_png(image, title, canonical_uri) {
                Ok(Some(profile)) => return Some(profile),
                Ok(None) => {}
                Err(err) => eprintln!(
                    "slide ai analysis failed for {}: {err}; falling back to heuristic profile",
                    observation.id
                ),
            }
        }

        None
    }

    pub(super) fn core_lock(&self) -> Result<std::sync::MutexGuard<'_, AppCore>, SelfHostError> {
        self.core.lock().map_err(|_| SelfHostError::LockPoisoned)
    }

    pub(super) fn persistence_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, SqlitePersistence>, SelfHostError> {
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
