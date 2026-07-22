use super::*;

const BULK_IMPORT_SESSION_FORMAT_VERSION: u32 = 1;
pub(super) const BULK_IMPORT_SESSION_STATE_KEY: &str = "bulk_import_session:v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkImportSessionPhase {
    Deferred,
    CatchingUp,
    Ready,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BulkImportSessionReport {
    pub session_id: String,
    pub state: BulkImportSessionPhase,
    pub base_append_seq: u64,
    pub target_append_seq: u64,
    pub target_observation_count: u64,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedBulkImportSession {
    format_version: u32,
    session_id: String,
    phase: BulkImportSessionPhase,
    base_append_seq: u64,
    target_append_seq: u64,
    target_observation_count: u64,
    started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

impl PersistedBulkImportSession {
    fn begin(stats: ObservationStats, now: DateTime<Utc>) -> Self {
        Self {
            format_version: BULK_IMPORT_SESSION_FORMAT_VERSION,
            session_id: format!("bulk:{}", uuid::Uuid::now_v7()),
            phase: BulkImportSessionPhase::Deferred,
            base_append_seq: stats.max_append_seq,
            target_append_seq: stats.max_append_seq,
            target_observation_count: stats.count,
            started_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    fn validate(&self) -> Result<(), SelfHostError> {
        if self.format_version != BULK_IMPORT_SESSION_FORMAT_VERSION {
            return Err(SelfHostError::Ingestion(format!(
                "unsupported persisted bulk import session format version {}",
                self.format_version
            )));
        }
        if self.session_id.trim().is_empty() {
            return Err(SelfHostError::Ingestion(
                "persisted bulk import session id must not be blank".to_owned(),
            ));
        }
        if self.target_append_seq < self.base_append_seq {
            return Err(SelfHostError::Ingestion(format!(
                "persisted bulk import session {} has target append sequence {} below base {}",
                self.session_id, self.target_append_seq, self.base_append_seq
            )));
        }
        match (self.phase, self.completed_at) {
            (BulkImportSessionPhase::Ready, None) => Err(SelfHostError::Ingestion(format!(
                "persisted completed bulk import session {} has no completed_at",
                self.session_id
            ))),
            (BulkImportSessionPhase::Deferred | BulkImportSessionPhase::CatchingUp, Some(_)) => {
                Err(SelfHostError::Ingestion(format!(
                    "persisted active bulk import session {} unexpectedly has completed_at",
                    self.session_id
                )))
            }
            _ => Ok(()),
        }
    }

    fn is_active(&self) -> bool {
        matches!(
            self.phase,
            BulkImportSessionPhase::Deferred | BulkImportSessionPhase::CatchingUp
        )
    }

    fn update_target(
        &mut self,
        stats: ObservationStats,
        now: DateTime<Utc>,
    ) -> Result<(), SelfHostError> {
        if self.phase != BulkImportSessionPhase::Deferred {
            return Err(SelfHostError::Ingestion(format!(
                "bulk import session {} cannot accept appends while {:?}",
                self.session_id, self.phase
            )));
        }
        if stats.max_append_seq < self.target_append_seq
            || stats.count < self.target_observation_count
        {
            return Err(SelfHostError::Ingestion(format!(
                "canonical observation high-water moved backwards during bulk import session {}",
                self.session_id
            )));
        }
        self.target_append_seq = stats.max_append_seq;
        self.target_observation_count = stats.count;
        self.updated_at = now;
        Ok(())
    }

    fn start_catch_up(
        &mut self,
        stats: ObservationStats,
        now: DateTime<Utc>,
    ) -> Result<(), SelfHostError> {
        self.update_target(stats, now)?;
        self.phase = BulkImportSessionPhase::CatchingUp;
        Ok(())
    }

    fn complete(
        &mut self,
        stats: ObservationStats,
        now: DateTime<Utc>,
    ) -> Result<(), SelfHostError> {
        if stats.max_append_seq < self.base_append_seq {
            return Err(SelfHostError::Ingestion(format!(
                "canonical observation high-water {} is below bulk import session {} base {}",
                stats.max_append_seq, self.session_id, self.base_append_seq
            )));
        }
        self.target_append_seq = stats.max_append_seq;
        self.target_observation_count = stats.count;
        self.phase = BulkImportSessionPhase::Ready;
        self.updated_at = now;
        self.completed_at = Some(now);
        Ok(())
    }

    fn report(&self) -> BulkImportSessionReport {
        BulkImportSessionReport {
            session_id: self.session_id.clone(),
            state: self.phase,
            base_append_seq: self.base_append_seq,
            target_append_seq: self.target_append_seq,
            target_observation_count: self.target_observation_count,
        }
    }
}

pub(super) fn load_persisted_bulk_import_session(
    persistence: &dyn StoragePorts,
) -> Result<Option<PersistedBulkImportSession>, SelfHostError> {
    let Some(raw) = persistence.get_state(BULK_IMPORT_SESSION_STATE_KEY)? else {
        return Ok(None);
    };
    let session = serde_json::from_str::<PersistedBulkImportSession>(&raw).map_err(|error| {
        SelfHostError::Ingestion(format!(
            "invalid persisted bulk import session state: {error}"
        ))
    })?;
    session.validate()?;
    Ok(Some(session))
}

pub(super) fn persist_bulk_import_session(
    persistence: &dyn StoragePorts,
    session: &PersistedBulkImportSession,
) -> Result<(), SelfHostError> {
    session.validate()?;
    let value = serde_json::to_string(session)?;
    persistence.set_state(BULK_IMPORT_SESSION_STATE_KEY, &value)?;
    Ok(())
}

impl AppService {
    pub(super) fn bulk_import_operation_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ()>, SelfHostError> {
        self.bulk_import_operation
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)
    }

    pub fn begin_bulk_import_session(&self) -> Result<BulkImportSessionReport, SelfHostError> {
        let _operation = self.bulk_import_operation_lock()?;
        let mut core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:person-page")?;
        let session = {
            let persistence = self.persistence_lock()?;
            if let Some(active) = load_persisted_bulk_import_session(persistence.as_ref())?
                && active.is_active()
            {
                return Err(SelfHostError::BulkImportSessionConflict {
                    code: "bulk_import_session_active",
                    detail: format!(
                        "bulk import session {} is already {:?}",
                        active.session_id, active.phase
                    ),
                });
            }
            let session =
                PersistedBulkImportSession::begin(persistence.observation_stats()?, Utc::now());
            persist_bulk_import_session(persistence.as_ref(), &session)?;
            session
        };
        core.mark_non_corpus_materializations_stale();
        let report = session.report();
        drop(core);
        self.emit_audit(
            "actor:self-host",
            AuditEventKind::WriteExecution,
            serde_json::json!({
                "mode": "bulk_import_session_begin",
                "session_id": report.session_id,
                "base_append_seq": report.base_append_seq,
            }),
        );
        Ok(report)
    }

    pub fn end_bulk_import_session(
        &self,
        session_id: &str,
    ) -> Result<BulkImportSessionReport, SelfHostError> {
        if session_id.trim().is_empty() {
            return Err(SelfHostError::BulkImportSessionConflict {
                code: "bulk_import_session_id_required",
                detail: "bulk import session id must not be blank".to_owned(),
            });
        }
        let _operation = self.bulk_import_operation_lock()?;
        let mut core = self.core_lock()?;
        let mut session = {
            let persistence = self.persistence_lock()?;
            let Some(mut session) = load_persisted_bulk_import_session(persistence.as_ref())?
            else {
                return Err(SelfHostError::BulkImportSessionConflict {
                    code: "bulk_import_session_not_active",
                    detail: "no bulk import session has been started".to_owned(),
                });
            };
            if session.session_id != session_id {
                return Err(SelfHostError::BulkImportSessionConflict {
                    code: "bulk_import_session_mismatch",
                    detail: format!(
                        "bulk import session {session_id} does not match current session {}",
                        session.session_id
                    ),
                });
            }
            if session.phase == BulkImportSessionPhase::Ready {
                return Ok(session.report());
            }
            let stats = persistence.observation_stats()?;
            match session.phase {
                BulkImportSessionPhase::Deferred => {
                    session.start_catch_up(stats, Utc::now())?;
                    persist_bulk_import_session(persistence.as_ref(), &session)?;
                }
                BulkImportSessionPhase::CatchingUp => {
                    if session.target_append_seq != stats.max_append_seq
                        || session.target_observation_count != stats.count
                    {
                        return Err(SelfHostError::Ingestion(format!(
                            "canonical observation high-water changed while bulk import session {} was catching up",
                            session.session_id
                        )));
                    }
                }
                BulkImportSessionPhase::Ready => unreachable!(),
            }
            session
        };

        core.mark_non_corpus_materializations_stale();
        self.search_index.catch_up_after_append()?;
        let person_page_ref = ProjectionRef::new("proj:person-page");
        let non_corpus_ready = core.catalog.get(&person_page_ref).is_some_and(|entry| {
            entry.status == ProjectionStatus::Active && entry.health == ProjectionHealth::Healthy
        }) && !self
            .non_corpus_rebuild_in_flight
            .load(std::sync::atomic::Ordering::Acquire);
        let target_already_materialized = non_corpus_ready
            && core.observation_stats.max_append_seq == session.target_append_seq
            && core.observation_stats.count == session.target_observation_count;
        if !target_already_materialized {
            self.refresh_materialized_snapshot(&mut core)?;
            drop(core);
            self.wait_for_non_corpus_rebuild()?;
            core = self.core_lock()?;
        }

        let ready_result = (|| {
            let stats = self.persistence_lock()?.observation_stats()?;
            if stats.max_append_seq != session.target_append_seq
                || stats.count != session.target_observation_count
            {
                return Err(SelfHostError::Ingestion(format!(
                    "canonical observation high-water changed before bulk import session {} publication",
                    session.session_id
                )));
            }
            session.complete(stats, Utc::now())?;
            persist_bulk_import_session(self.persistence_lock()?.as_ref(), &session)
        })();
        if let Err(error) = ready_result {
            core.mark_non_corpus_materializations_stale();
            return Err(error);
        }
        if target_already_materialized {
            core.activate_non_corpus_projections();
        }
        let report = session.report();
        drop(core);
        self.emit_audit(
            "actor:self-host",
            AuditEventKind::WriteExecution,
            serde_json::json!({
                "mode": "bulk_import_session_end",
                "session_id": report.session_id,
                "target_append_seq": report.target_append_seq,
                "target_observation_count": report.target_observation_count,
            }),
        );
        Ok(report)
    }

    pub(super) fn bulk_import_session_for_append(
        &self,
        requested_session_id: Option<&str>,
    ) -> Result<Option<PersistedBulkImportSession>, SelfHostError> {
        if requested_session_id.is_some_and(|session_id| session_id.trim().is_empty()) {
            return Err(SelfHostError::BulkImportSessionConflict {
                code: "bulk_import_session_id_required",
                detail: "bulk import session id must not be blank".to_owned(),
            });
        }
        let persistence = self.persistence_lock()?;
        let persisted = load_persisted_bulk_import_session(persistence.as_ref())?;
        match (persisted, requested_session_id) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(SelfHostError::BulkImportSessionConflict {
                code: "bulk_import_session_not_active",
                detail: "no bulk import session has been started".to_owned(),
            }),
            (Some(session), None) if !session.is_active() => Ok(None),
            (Some(session), Some(requested)) if !session.is_active() => {
                Err(SelfHostError::BulkImportSessionConflict {
                    code: "bulk_import_session_not_active",
                    detail: format!(
                        "bulk import session {requested} is not active; last completed session is {}",
                        session.session_id
                    ),
                })
            }
            (Some(session), None) => Err(SelfHostError::BulkImportSessionConflict {
                code: "bulk_import_session_id_required",
                detail: format!(
                    "bulk import session {} is active; import request must include bulk_session_id",
                    session.session_id
                ),
            }),
            (Some(session), Some(requested)) if session.session_id != requested => {
                Err(SelfHostError::BulkImportSessionConflict {
                    code: "bulk_import_session_mismatch",
                    detail: format!(
                        "bulk import session {requested} does not match active session {}",
                        session.session_id
                    ),
                })
            }
            (Some(session), Some(_)) if session.phase == BulkImportSessionPhase::Deferred => {
                Ok(Some(session))
            }
            (Some(session), Some(_)) => Err(SelfHostError::BulkImportSessionConflict {
                code: "bulk_import_session_catching_up",
                detail: format!(
                    "bulk import session {} is catching up and no longer accepts appends",
                    session.session_id
                ),
            }),
        }
    }

    pub(super) fn record_deferred_bulk_import_append(
        &self,
        mut session: PersistedBulkImportSession,
    ) -> Result<(), SelfHostError> {
        let persistence = self.persistence_lock()?;
        let stats = persistence.observation_stats()?;
        session.update_target(stats, Utc::now())?;
        persist_bulk_import_session(persistence.as_ref(), &session)
    }

    pub(super) fn ensure_bulk_import_session_inactive(
        &self,
        operation: &str,
    ) -> Result<(), SelfHostError> {
        let persistence = self.persistence_lock()?;
        if let Some(session) = load_persisted_bulk_import_session(persistence.as_ref())?
            && session.is_active()
        {
            return Err(SelfHostError::BulkImportSessionConflict {
                code: "bulk_import_session_active",
                detail: format!(
                    "{operation} is unavailable while bulk import session {} is {:?}",
                    session.session_id, session.phase
                ),
            });
        }
        Ok(())
    }

    pub(super) fn bulk_import_health_dependency(
        &self,
    ) -> Result<DependencyHealthInfo, SelfHostError> {
        let persistence = self.persistence_lock()?;
        let state = load_persisted_bulk_import_session(persistence.as_ref())?;
        let Some(session) = state else {
            return Ok(DependencyHealthInfo {
                name: "bulk_import_session".to_owned(),
                status: "ok".to_owned(),
                detail: None,
            });
        };
        if !session.is_active() {
            return Ok(DependencyHealthInfo {
                name: "bulk_import_session".to_owned(),
                status: "ok".to_owned(),
                detail: None,
            });
        }
        let stats = persistence.observation_stats()?;
        let lag = stats
            .max_append_seq
            .checked_sub(session.base_append_seq)
            .ok_or_else(|| {
                SelfHostError::Ingestion(format!(
                    "canonical high-water is below bulk import session {} base",
                    session.session_id
                ))
            })?;
        Ok(DependencyHealthInfo {
            name: "bulk_import_session".to_owned(),
            status: match session.phase {
                BulkImportSessionPhase::Deferred => "deferred",
                BulkImportSessionPhase::CatchingUp => "catching_up",
                BulkImportSessionPhase::Ready => unreachable!(),
            }
            .to_owned(),
            detail: Some(format!(
                "session_id={} projection_high_water={} canonical_high_water={} lag={lag}",
                session.session_id, session.base_append_seq, stats.max_append_seq
            )),
        })
    }
}
