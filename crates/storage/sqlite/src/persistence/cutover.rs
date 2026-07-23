use super::*;

use std::collections::BTreeMap;

const CUTOVER_UNIT_META_KEY: &str = "source_instance";
const OBJECT_ID_META_KEY: &str = "object_id";
const LEGACY_HISTORY_SCHEMA: &str = "schema:history-message";
const LEGACY_HISTORY_SOURCE_INSTANCE_META_KEY: &str = "source_instance_id";
const CUTOVER_PRODUCER_META_KEYS: &[&str] = &["producer_id", "producer"];
const CUTOVER_CREDENTIAL_META_KEYS: &[&str] = &["credential_id", "credential_ref"];

fn validate_cutover_unit(source_instance_id: &str) -> Result<(), PersistenceError> {
    if source_instance_id.trim().is_empty() {
        return Err(PersistenceError::SchemaInvariant(
            "source_instance_id must not be blank".to_owned(),
        ));
    }
    Ok(())
}

fn parse_phase(value: &str) -> Result<CutoverPhase, PersistenceError> {
    match value {
        "v1_active" => Ok(CutoverPhase::V1Active),
        "draining" => Ok(CutoverPhase::Draining),
        "v2_active" => Ok(CutoverPhase::V2Active),
        "v2_committed" => Ok(CutoverPhase::V2Committed),
        other => Err(PersistenceError::SchemaInvariant(format!(
            "unknown cutover phase {other:?}"
        ))),
    }
}

fn bridge_identity_key(source_instance_id: &str, object_id: &str, canonical_json: &str) -> String {
    format!(
        "{source_instance_id}:{object_id}:{}",
        hex::encode(sha2::Sha256::digest(canonical_json.as_bytes()))
    )
}

fn observation_identity_inputs(
    observation: &Observation,
) -> Result<(String, String, String), String> {
    let meta = observation
        .meta
        .as_object()
        .ok_or_else(|| "meta is not an object".to_owned())?;
    let canonical_json = meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "meta.canonical_json is missing or blank".to_owned())?;
    let canonical_value = serde_json::from_str::<serde_json::Value>(canonical_json)
        .map_err(|_| "meta.canonical_json is not valid JSON")?;
    let source_instance_id = meta
        .get(CUTOVER_UNIT_META_KEY)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            (observation.schema.as_str() == LEGACY_HISTORY_SCHEMA)
                .then(|| {
                    meta.get(LEGACY_HISTORY_SOURCE_INSTANCE_META_KEY)
                        .and_then(serde_json::Value::as_str)
                        .filter(|value| !value.trim().is_empty())
                        .map(str::to_owned)
                })
                .flatten()
        })
        .ok_or_else(|| "meta.source_instance is missing or blank".to_owned())?;
    let object_id = meta
        .get(OBJECT_ID_META_KEY)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            (observation.schema.as_str() == LEGACY_HISTORY_SCHEMA)
                .then(|| {
                    let session_id = canonical_value
                        .get("source_session_id")
                        .and_then(serde_json::Value::as_str)
                        .filter(|value| !value.trim().is_empty())?;
                    let message_id = canonical_value
                        .get("source_message_id")
                        .and_then(serde_json::Value::as_str)
                        .filter(|value| !value.trim().is_empty())?;
                    Some(format!("{session_id}:{message_id}"))
                })
                .flatten()
        })
        .ok_or_else(|| "meta.object_id is missing or blank".to_owned())?;
    Ok((source_instance_id, object_id, canonical_json.to_owned()))
}

fn cutover_state_connection(
    connection: &Connection,
    source_instance_id: &str,
) -> Result<Option<CutoverState>, PersistenceError> {
    let mut statement = connection.prepare(
        "SELECT from_phase, to_phase, generation, fence_append_seq, first_v2_append_seq
         FROM cutover_transition_log
         WHERE source_instance_id = ?1
         ORDER BY event_seq",
    )?;
    let rows = statement
        .query_map([source_instance_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u64>(2)?,
                row.get::<_, Option<u64>>(3)?,
                row.get::<_, Option<u64>>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(None);
    }
    let mut current_phase: Option<CutoverPhase> = None;
    let mut current_generation = 0_u64;
    let mut fence_append_seq = None;
    let mut first_v2_append_seq = None;
    for (from_phase, to_phase, generation, fence, first) in &rows {
        let to_phase = parse_phase(to_phase)?;
        match current_phase {
            None => {
                if from_phase != "uninitialized"
                    || to_phase != CutoverPhase::V1Active
                    || *generation != 1
                {
                    return Err(PersistenceError::SchemaInvariant(format!(
                        "invalid initial cutover transition for {source_instance_id}"
                    )));
                }
            }
            Some(previous_phase) => {
                if from_phase != previous_phase.as_str() {
                    return Err(PersistenceError::SchemaInvariant(format!(
                        "cutover transition for {source_instance_id} starts from {from_phase}, expected {}",
                        previous_phase.as_str()
                    )));
                }
                let valid = match (previous_phase, to_phase) {
                    (CutoverPhase::V1Active, CutoverPhase::Draining) => {
                        *generation == current_generation
                    }
                    (CutoverPhase::Draining, CutoverPhase::V2Active)
                    | (CutoverPhase::Draining, CutoverPhase::V1Active)
                    | (CutoverPhase::V2Active, CutoverPhase::V1Active) => {
                        *generation == current_generation.saturating_add(1)
                    }
                    (CutoverPhase::V2Active, CutoverPhase::V2Committed) => {
                        *generation == current_generation && first.is_some()
                    }
                    _ => false,
                };
                if !valid {
                    return Err(PersistenceError::SchemaInvariant(format!(
                        "invalid cutover transition {} -> {} for {source_instance_id}",
                        previous_phase.as_str(),
                        to_phase.as_str()
                    )));
                }
            }
        }
        current_phase = Some(to_phase);
        current_generation = *generation;
        if fence.is_some() {
            fence_append_seq = *fence;
        }
        if first.is_some() {
            first_v2_append_seq = *first;
        }
    }
    let last_phase = current_phase.ok_or_else(|| {
        PersistenceError::SchemaInvariant(format!(
            "cutover state fold produced no state for {source_instance_id}"
        ))
    })?;
    let state = CutoverState {
        source_instance_id: source_instance_id.to_owned(),
        phase: last_phase,
        generation: current_generation,
        fence_append_seq,
        first_v2_append_seq,
        v2_ingested: connection
            .query_row(
                "SELECT v2_ingested FROM cutover_unit_metrics WHERE source_instance_id = ?1",
                [source_instance_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0),
    };
    Ok(Some(state))
}

fn required_state_connection(
    connection: &Connection,
    source_instance_id: &str,
) -> Result<CutoverState, PersistenceError> {
    cutover_state_connection(connection, source_instance_id)?.ok_or_else(|| {
        PersistenceError::SchemaInvariant(format!(
            "cutover unit {source_instance_id} is not registered"
        ))
    })
}

fn required_state_transaction(
    transaction: &rusqlite::Transaction<'_>,
    source_instance_id: &str,
) -> Result<CutoverState, PersistenceError> {
    required_state_connection(transaction, source_instance_id)
}

#[allow(clippy::too_many_arguments)]
fn record_transition(
    transaction: &rusqlite::Transaction<'_>,
    source_instance_id: &str,
    from_phase: &str,
    to_phase: CutoverPhase,
    authority: &str,
    reason: &str,
    generation: u64,
    fence_append_seq: Option<u64>,
    first_v2_append_seq: Option<u64>,
) -> Result<(), PersistenceError> {
    if authority.trim().is_empty() || reason.trim().is_empty() {
        return Err(PersistenceError::SchemaInvariant(
            "cutover transition authority and reason must not be blank".to_owned(),
        ));
    }
    transaction.execute(
        "INSERT INTO cutover_transition_log (
            source_instance_id, from_phase, to_phase, authority, reason,
            generation, fence_append_seq, first_v2_append_seq, committed_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            source_instance_id,
            from_phase,
            to_phase.as_str(),
            authority,
            reason,
            generation,
            fence_append_seq,
            first_v2_append_seq,
            chrono::Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn set_active_credential(
    transaction: &rusqlite::Transaction<'_>,
    source_instance_id: &str,
    api_version: CutoverApiVersion,
    generation: u64,
) -> Result<(), PersistenceError> {
    transaction.execute(
        "UPDATE cutover_credentials
         SET active = 0
         WHERE source_instance_id = ?1",
        [source_instance_id],
    )?;
    transaction.execute(
        "INSERT INTO cutover_credentials (
            source_instance_id, api_version, generation, credential_ref, active, issued_at
         ) VALUES (?1, ?2, ?3, ?4, 1, ?5)",
        params![
            source_instance_id,
            api_version.as_str(),
            generation,
            format!(
                "unit:{source_instance_id}:{}:{generation}",
                api_version.as_str()
            ),
            chrono::Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn ensure_metrics_row(
    transaction: &rusqlite::Transaction<'_>,
    source_instance_id: &str,
) -> Result<(), PersistenceError> {
    transaction.execute(
        "INSERT OR IGNORE INTO cutover_unit_metrics (
            source_instance_id, bridge_duplicate_hits, stale_v1_rejections,
            v2_ingested, updated_at
         ) VALUES (?1, 0, 0, 0, ?2)",
        params![source_instance_id, chrono::Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn update_metric(
    transaction: &rusqlite::Transaction<'_>,
    source_instance_id: &str,
    column: &str,
    amount: u64,
) -> Result<(), PersistenceError> {
    let allowed = [
        "bridge_duplicate_hits",
        "stale_v1_rejections",
        "v2_ingested",
    ];
    if !allowed.contains(&column) {
        return Err(PersistenceError::SchemaInvariant(format!(
            "unknown cutover metric column {column}"
        )));
    }
    let sql = format!(
        "UPDATE cutover_unit_metrics SET {column} = {column} + ?1, updated_at = ?2
         WHERE source_instance_id = ?3"
    );
    transaction.execute(
        &sql,
        params![amount, chrono::Utc::now().to_rfc3339(), source_instance_id],
    )?;
    Ok(())
}

fn admission_denial(
    transaction: &rusqlite::Transaction<'_>,
    source_instance_id: &str,
    api_version: CutoverApiVersion,
    generation: Option<u64>,
) -> Result<Option<String>, PersistenceError> {
    let Some(state) = cutover_state_connection(transaction, source_instance_id)? else {
        return Ok(None);
    };
    let expected_phase = match api_version {
        CutoverApiVersion::V1 => CutoverPhase::V1Active,
        CutoverApiVersion::V2 => CutoverPhase::V2Active,
    };
    if state.phase != expected_phase
        && !(api_version == CutoverApiVersion::V2 && state.phase == CutoverPhase::V2Committed)
    {
        return Ok(Some(format!(
            "unit {source_instance_id} is {}, not admitting {}",
            state.phase.as_str(),
            api_version.as_str()
        )));
    }
    if generation != Some(state.generation) {
        return Ok(Some(format!(
            "{} credential generation is stale or missing for unit {source_instance_id}: expected {}, got {:?}",
            api_version.as_str(),
            state.generation,
            generation
        )));
    }
    let active = transaction.query_row(
        "SELECT EXISTS (
                 SELECT 1 FROM cutover_credentials
                 WHERE source_instance_id = ?1 AND api_version = ?2
                   AND generation = ?3 AND active = 1
             )",
        params![source_instance_id, api_version.as_str(), state.generation],
        |row| row.get::<_, bool>(0),
    )?;
    if !active {
        return Ok(Some(format!(
            "{} credential generation {} is not active for unit {source_instance_id}",
            api_version.as_str(),
            state.generation
        )));
    }
    Ok(None)
}

fn bridge_resolution_transaction(
    transaction: &rusqlite::Transaction<'_>,
    v2_identity_key: &str,
    canonical_json: &str,
) -> Result<IdentityBridgeResolution, PersistenceError> {
    let mut statement = transaction.prepare(
        "SELECT observation_id, append_seq, canonical_json
         FROM identity_bridge_candidates
         WHERE v2_identity_key = ?1
         ORDER BY append_seq, observation_id",
    )?;
    let candidates = statement
        .query_map([v2_identity_key], |row| {
            Ok((
                ObservationId::new(row.get::<_, String>(0)?),
                row.get::<_, u64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let winner = candidates.first().cloned();
    let canonical_collision = candidates
        .iter()
        .any(|(_, _, candidate_json)| candidate_json != canonical_json);
    Ok(IdentityBridgeResolution {
        v2_identity_key: v2_identity_key.to_owned(),
        winner: winner.as_ref().map(|(id, _, _)| id.clone()),
        winner_append_seq: winner.as_ref().map(|(_, append_seq, _)| *append_seq),
        multiplicity: u64::try_from(candidates.len()).map_err(|_| {
            PersistenceError::SchemaInvariant("bridge candidate count overflow".to_owned())
        })?,
        canonical_collision,
        collision_append_seq: candidates
            .iter()
            .find(|(_, _, candidate_json)| candidate_json != canonical_json)
            .map(|(_, append_seq, _)| *append_seq),
    })
}

fn readiness_transaction(
    transaction: &rusqlite::Transaction<'_>,
    source_instance_id: &str,
    fixture: Option<&CutoverFixture>,
) -> Result<CutoverReadinessReport, PersistenceError> {
    let state = required_state_transaction(transaction, source_instance_id)?;
    let watermark = transaction.query_row(
        "SELECT last_append_seq FROM identity_bridge_watermark WHERE singleton = 1",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let fence = state.fence_append_seq.unwrap_or(0);
    let watermark_covered = state.fence_append_seq.is_some() && watermark >= fence;
    let bridge_lag = fence.saturating_sub(watermark);
    let unresolved_gap_count = transaction.query_row(
        "SELECT COUNT(*) FROM identity_bridge_gaps
         WHERE append_seq <= ?1 AND (source_instance_id = ?2 OR source_instance_id IS NULL)",
        params![fence, source_instance_id],
        |row| row.get::<_, u64>(0),
    )?;
    let exact_compare_error_count = transaction.query_row(
        "SELECT COUNT(*) FROM (
             SELECT v2_identity_key
             FROM identity_bridge_candidates
             WHERE source_instance_id = ?1
             GROUP BY v2_identity_key
             HAVING COUNT(DISTINCT canonical_json) > 1
         )",
        [source_instance_id],
        |row| row.get::<_, u64>(0),
    )?;
    let candidate_count = transaction.query_row(
        "SELECT COUNT(*) FROM identity_bridge_candidates WHERE source_instance_id = ?1",
        [source_instance_id],
        |row| row.get::<_, u64>(0),
    )?;
    let multiplicity_count = transaction.query_row(
        "SELECT COUNT(*) FROM (
             SELECT v2_identity_key
             FROM identity_bridge_candidates
             WHERE source_instance_id = ?1
             GROUP BY v2_identity_key
             HAVING COUNT(*) > 1
         )",
        [source_instance_id],
        |row| row.get::<_, u64>(0),
    )?;
    let collision_count = transaction.query_row(
        "SELECT COUNT(*) FROM (
             SELECT v2_identity_key
             FROM identity_bridge_candidates
             WHERE source_instance_id = ?1
             GROUP BY v2_identity_key
             HAVING COUNT(DISTINCT canonical_json) > 1
         )",
        [source_instance_id],
        |row| row.get::<_, u64>(0),
    )?;
    let (fixture_identity_stable, dry_run_passed, fixture_blocker) = match fixture {
        Some(fixture) => {
            let identity_stable =
                serde_json::from_str::<serde_json::Value>(&fixture.canonical_json).is_ok()
                    && fixture.expected_identity_key
                        == bridge_identity_key(
                            source_instance_id,
                            &fixture.object_id,
                            &fixture.canonical_json,
                        );
            let resolution = bridge_resolution_transaction(
                transaction,
                &fixture.expected_identity_key,
                &fixture.canonical_json,
            )?;
            let dry_run = identity_stable
                && resolution.winner.is_some()
                && !resolution.canonical_collision
                && fixture
                    .expected_observation_id
                    .as_ref()
                    .is_none_or(|expected| resolution.winner.as_ref() == Some(expected));
            let blocker = if identity_stable && dry_run {
                None
            } else {
                Some("retry fixture identity or existing-id dry-run failed".to_owned())
            };
            (identity_stable, dry_run, blocker)
        }
        None => (false, false, Some("retry fixture is required".to_owned())),
    };

    let mut blockers = Vec::new();
    if !watermark_covered {
        blockers.push(CutoverBlocker {
            append_seq: state.fence_append_seq,
            reason: format!("bridge watermark {watermark} is below fence append_seq {fence}"),
        });
    }
    if unresolved_gap_count > 0 {
        blockers.push(CutoverBlocker {
            append_seq: Some(fence),
            reason: format!("{unresolved_gap_count} unresolved identity derivation gap(s)"),
        });
    }
    if collision_count > 0 {
        blockers.push(CutoverBlocker {
            append_seq: None,
            reason: format!("{collision_count} canonical exact-compare collision group(s)"),
        });
    }
    if let Some(reason) = fixture_blocker {
        blockers.push(CutoverBlocker {
            append_seq: None,
            reason,
        });
    }
    if state.phase != CutoverPhase::Draining {
        blockers.push(CutoverBlocker {
            append_seq: state.fence_append_seq,
            reason: format!("unit is in {}, expected draining", state.phase.as_str()),
        });
    }
    Ok(CutoverReadinessReport {
        state,
        bridge_watermark: watermark,
        bridge_lag,
        watermark_covered,
        unresolved_gap_count,
        exact_compare_error_count,
        fixture_identity_stable,
        dry_run_passed,
        candidate_count,
        multiplicity_count,
        collision_count,
        ready: blockers.is_empty(),
        blockers,
    })
}

impl SqlitePersistence {
    pub(super) fn append_v2_observation_for_operational_event(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        tree: &PartitionTree,
        source_instance_id: &str,
        observation: &Observation,
    ) -> Result<(DurableAppendOutcome, bool), PersistenceError> {
        let (observed_source, object_id, canonical_json) = observation_identity_inputs(observation)
            .map_err(|reason| {
                PersistenceError::SchemaInvariant(format!(
                    "v2 bridge input for {} is invalid: {reason}",
                    observation.id
                ))
            })?;
        if observed_source != source_instance_id {
            return Err(PersistenceError::SchemaInvariant(format!(
                "v2 observation {} belongs to source_instance_id {}, expected {}",
                observation.id, observed_source, source_instance_id
            )));
        }
        let v2_identity_key = bridge_identity_key(source_instance_id, &object_id, &canonical_json);
        if observation.idempotency_key.as_str() != v2_identity_key {
            return Err(PersistenceError::SchemaInvariant(format!(
                "v2 observation {} identity does not match bridge formula",
                observation.id
            )));
        }
        let resolution =
            bridge_resolution_transaction(transaction, &v2_identity_key, &canonical_json)?;
        if let Some(existing_id) = resolution.winner {
            return Ok((
                if resolution.canonical_collision {
                    DurableAppendOutcome::CanonicalCollision(existing_id)
                } else {
                    DurableAppendOutcome::Duplicate(existing_id)
                },
                true,
            ));
        }
        let mut appended = append_observations_in_transaction(
            transaction,
            tree,
            self.routing_key_order,
            std::slice::from_ref(observation),
        )?;
        Ok((appended.remove(0), false))
    }

    pub(super) fn record_v2_append_metrics(
        transaction: &rusqlite::Transaction<'_>,
        source_instance_id: &str,
        bridge_hits: u64,
        appended_ids: &[ObservationId],
        authority: &str,
        reason: &str,
    ) -> StorageResult<()> {
        if cutover_state_connection(transaction, source_instance_id)
            .map_err(storage_error)?
            .is_none()
        {
            return Ok(());
        }
        ensure_metrics_row(transaction, source_instance_id).map_err(storage_error)?;
        if bridge_hits > 0 {
            update_metric(
                transaction,
                source_instance_id,
                "bridge_duplicate_hits",
                bridge_hits,
            )
            .map_err(storage_error)?;
        }
        if appended_ids.is_empty() {
            return Ok(());
        }
        let appended_count = u64::try_from(appended_ids.len())
            .map_err(|_| StorageError::Invariant("v2 ingested count overflow".to_owned()))?;
        let state =
            required_state_transaction(transaction, source_instance_id).map_err(storage_error)?;
        update_metric(
            transaction,
            source_instance_id,
            "v2_ingested",
            appended_count,
        )
        .map_err(storage_error)?;
        if state.phase == CutoverPhase::V2Active {
            let first_append_seq = appended_ids
                .iter()
                .map(|id| {
                    transaction
                        .query_row(
                            "SELECT append_seq FROM observations WHERE id = ?1",
                            [id.as_str()],
                            |row| row.get::<_, u64>(0),
                        )
                        .map_err(PersistenceError::from)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_error)?
                .into_iter()
                .min();
            record_transition(
                transaction,
                source_instance_id,
                state.phase.as_str(),
                CutoverPhase::V2Committed,
                authority,
                reason,
                state.generation,
                state.fence_append_seq,
                first_append_seq,
            )
            .map_err(storage_error)?;
        }
        Ok(())
    }

    fn append_v2_in_transaction(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        tree: &PartitionTree,
        source_instance_id: &str,
        observations: &[Observation],
    ) -> Result<(Vec<DurableAppendOutcome>, u64), PersistenceError> {
        let mut outcomes = Vec::with_capacity(observations.len());
        let mut bridge_hits = 0_u64;
        for observation in observations {
            let (outcome, bridge_hit) = self.append_v2_observation_for_operational_event(
                transaction,
                tree,
                source_instance_id,
                observation,
            )?;
            if bridge_hit && matches!(outcome, DurableAppendOutcome::Duplicate(_)) {
                bridge_hits = bridge_hits.checked_add(1).ok_or_else(|| {
                    PersistenceError::SchemaInvariant("bridge hit count overflow".to_owned())
                })?;
            }
            outcomes.push(outcome);
        }
        Ok((outcomes, bridge_hits))
    }

    pub(super) fn cutover_admit_transaction(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        source_instance_id: &str,
        api_version: CutoverApiVersion,
        generation: Option<u64>,
    ) -> Result<(), PersistenceError> {
        if let Some(reason) =
            admission_denial(transaction, source_instance_id, api_version, generation)?
        {
            if api_version == CutoverApiVersion::V1
                && cutover_state_connection(transaction, source_instance_id)?.is_some()
            {
                update_metric(transaction, source_instance_id, "stale_v1_rejections", 1)?;
            }
            return Err(PersistenceError::CutoverAdmissionDenied(reason));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn identity_bridge_apply_batch_with_failure_for_test(
        &self,
        batch_size: usize,
    ) -> StorageResult<IdentityBridgeBatchReport> {
        self.conn
            .execute_batch(
                "
                CREATE TRIGGER cutover_bridge_test_fail_watermark
                BEFORE UPDATE OF last_append_seq ON identity_bridge_watermark
                BEGIN
                    SELECT RAISE(ABORT, 'injected bridge watermark failure');
                END;
                ",
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let result = self.identity_bridge_apply_batch(batch_size);
        self.conn
            .execute_batch("DROP TRIGGER cutover_bridge_test_fail_watermark")
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        result
    }
}

impl CutoverStore for SqlitePersistence {
    fn append_observations_v1_with_admission(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observations: &[Observation],
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> StorageResult<Vec<PortAppendOutcome>> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let tree = self.partition_tree_snapshot().map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        if let Err(error) = self.cutover_admit_transaction(
            &transaction,
            source_instance_id,
            CutoverApiVersion::V1,
            generation,
        ) {
            transaction
                .commit()
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            return Err(storage_error(error));
        }
        let outcomes = append_observations_in_transaction(
            &transaction,
            &tree,
            self.routing_key_order,
            observations,
        )
        .map_err(storage_error)?;
        for audit in audit_events {
            insert_audit_event(&transaction, audit).map_err(storage_error)?;
        }
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        Ok(outcomes.into_iter().map(port_outcome).collect())
    }

    fn append_slack_observation_v1_with_admission(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observation: &Observation,
        thread: &SlackThreadKey,
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> StorageResult<PortAppendOutcome> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        validate_slack_thread_key(thread).map_err(storage_error)?;
        let tree = self.partition_tree_snapshot().map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        if let Err(error) = self.cutover_admit_transaction(
            &transaction,
            source_instance_id,
            CutoverApiVersion::V1,
            generation,
        ) {
            transaction
                .commit()
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            return Err(storage_error(error));
        }
        let mut outcomes = append_observations_in_transaction(
            &transaction,
            &tree,
            self.routing_key_order,
            std::slice::from_ref(observation),
        )
        .map_err(storage_error)?;
        let outcome = outcomes.remove(0);
        if let DurableAppendOutcome::Appended(observation_id)
        | DurableAppendOutcome::Duplicate(observation_id) = &outcome
        {
            let append_seq = transaction
                .query_row(
                    "SELECT append_seq FROM observations WHERE id = ?1",
                    [observation_id.as_str()],
                    |row| row.get::<_, u64>(0),
                )
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            upsert_slack_thread(&transaction, thread, append_seq).map_err(storage_error)?;
        }
        for audit in audit_events {
            insert_audit_event(&transaction, audit).map_err(storage_error)?;
        }
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        Ok(port_outcome(outcome))
    }

    fn append_observations_v2_with_bridge(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observations: &[Observation],
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> StorageResult<Vec<PortAppendOutcome>> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let tree = self.partition_tree_snapshot().map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        if let Err(error) = self.cutover_admit_transaction(
            &transaction,
            source_instance_id,
            CutoverApiVersion::V2,
            generation,
        ) {
            transaction
                .commit()
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            return Err(storage_error(error));
        }
        let (outcomes, bridge_hits) = self
            .append_v2_in_transaction(&transaction, &tree, source_instance_id, observations)
            .map_err(storage_error)?;
        for audit in audit_events {
            insert_audit_event(&transaction, audit).map_err(storage_error)?;
        }
        let appended_ids = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                DurableAppendOutcome::Appended(id) => Some(id.clone()),
                DurableAppendOutcome::Duplicate(_)
                | DurableAppendOutcome::CanonicalCollision(_) => None,
            })
            .collect::<Vec<_>>();
        Self::record_v2_append_metrics(
            &transaction,
            source_instance_id,
            bridge_hits,
            &appended_ids,
            "actor:self-host",
            "first v2 ingested observation committed",
        )?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        Ok(outcomes.into_iter().map(port_outcome).collect())
    }

    fn cutover_admit(
        &self,
        source_instance_id: &str,
        api_version: CutoverApiVersion,
        generation: Option<u64>,
    ) -> StorageResult<()> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        if let Err(error) = self.cutover_admit_transaction(
            &transaction,
            source_instance_id,
            api_version,
            generation,
        ) {
            transaction
                .commit()
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            return Err(storage_error(error));
        }
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)
    }

    fn identity_bridge_apply_batch(
        &self,
        batch_size: usize,
    ) -> StorageResult<IdentityBridgeBatchReport> {
        if batch_size == 0 {
            return Err(StorageError::Invariant(
                "identity bridge batch size must be greater than zero".to_owned(),
            ));
        }
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let previous = transaction
            .query_row(
                "SELECT last_append_seq FROM identity_bridge_watermark WHERE singleton = 1",
                [],
                |row| row.get::<_, u64>(0),
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let rows = {
            let mut statement = transaction
                .prepare(
                    "SELECT append_seq, id, observation_json
                     FROM observations
                     WHERE append_seq > ?1
                     ORDER BY append_seq
                     LIMIT ?2",
                )
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            statement
                .query_map(params![previous, batch_size], |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        ObservationId::new(row.get::<_, String>(1)?),
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(PersistenceError::from)
                .map_err(storage_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(PersistenceError::from)
                .map_err(storage_error)?
        };
        if rows.is_empty() {
            transaction
                .commit()
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            return Ok(IdentityBridgeBatchReport {
                previous_watermark: previous,
                watermark: previous,
                read_count: 0,
                candidate_count: 0,
                gap_count: 0,
            });
        }
        let mut candidate_count = 0_usize;
        let mut gap_count = 0_usize;
        let mut last_append_seq = previous;
        for (append_seq, observation_id, observation_json) in &rows {
            let observation: Observation = serde_json::from_str(observation_json)
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            match observation_identity_inputs(&observation) {
                Ok((source_instance_id, object_id, canonical_json)) => {
                    let v2_identity_key =
                        bridge_identity_key(&source_instance_id, &object_id, &canonical_json);
                    if observation.idempotency_key.as_str() != v2_identity_key {
                        let inserted = transaction
                            .execute(
                                "INSERT OR IGNORE INTO identity_bridge_candidates (
                                    v2_identity_key, observation_id, source_instance_id,
                                    append_seq, canonical_json, canonical_json_sha256
                                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                params![
                                    v2_identity_key,
                                    observation_id.as_str(),
                                    source_instance_id,
                                    append_seq,
                                    canonical_json,
                                    canonical_json_sha256(&canonical_json),
                                ],
                            )
                            .map_err(PersistenceError::from)
                            .map_err(storage_error)?;
                        candidate_count += inserted;
                    }
                }
                Err(reason) => {
                    let source_instance_id = observation
                        .meta
                        .as_object()
                        .and_then(|meta| meta.get(CUTOVER_UNIT_META_KEY))
                        .and_then(serde_json::Value::as_str);
                    let inserted = transaction
                        .execute(
                            "INSERT OR IGNORE INTO identity_bridge_gaps (
                                append_seq, observation_id, source_instance_id, reason
                             ) VALUES (?1, ?2, ?3, ?4)",
                            params![
                                append_seq,
                                observation_id.as_str(),
                                source_instance_id,
                                reason
                            ],
                        )
                        .map_err(PersistenceError::from)
                        .map_err(storage_error)?;
                    gap_count += inserted;
                }
            }
            last_append_seq = *append_seq;
        }
        transaction
            .execute(
                "UPDATE identity_bridge_watermark SET last_append_seq = ?1 WHERE singleton = 1",
                [last_append_seq],
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        Ok(IdentityBridgeBatchReport {
            previous_watermark: previous,
            watermark: last_append_seq,
            read_count: rows.len(),
            candidate_count,
            gap_count,
        })
    }

    fn identity_bridge_watermark(&self) -> StorageResult<u64> {
        self.conn
            .query_row(
                "SELECT last_append_seq FROM identity_bridge_watermark WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)
    }

    fn identity_bridge_resolve(
        &self,
        v2_identity_key: &str,
        canonical_json: &str,
    ) -> StorageResult<IdentityBridgeResolution> {
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let result = bridge_resolution_transaction(&transaction, v2_identity_key, canonical_json)
            .map_err(storage_error)?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        Ok(result)
    }

    fn cutover_register(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        if let Some(state) =
            cutover_state_connection(&transaction, source_instance_id).map_err(storage_error)?
        {
            transaction
                .commit()
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            return Ok(state);
        }
        record_transition(
            &transaction,
            source_instance_id,
            "uninitialized",
            CutoverPhase::V1Active,
            authority,
            reason,
            1,
            None,
            None,
        )
        .map_err(storage_error)?;
        set_active_credential(&transaction, source_instance_id, CutoverApiVersion::V1, 1)
            .map_err(storage_error)?;
        ensure_metrics_row(&transaction, source_instance_id).map_err(storage_error)?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        self.cutover_state(source_instance_id)
    }

    fn cutover_state(&self, source_instance_id: &str) -> StorageResult<CutoverState> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        required_state_connection(&self.conn, source_instance_id).map_err(storage_error)
    }

    fn cutover_inventory(&self) -> StorageResult<Vec<CutoverInventoryItem>> {
        let mut units = BTreeMap::<String, CutoverInventoryItem>::new();
        let mut statement = self
            .conn
            .prepare("SELECT observation_json FROM observations ORDER BY append_seq")
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(PersistenceError::from)
            .map_err(storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        for json in rows {
            let observation: Observation = serde_json::from_str(&json)
                .map_err(PersistenceError::from)
                .map_err(storage_error)?;
            let Some(meta) = observation.meta.as_object() else {
                continue;
            };
            let Some(source_instance_id) = meta
                .get(CUTOVER_UNIT_META_KEY)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
            else {
                continue;
            };
            let item = units
                .entry(source_instance_id.to_owned())
                .or_insert_with(|| CutoverInventoryItem {
                    source_instance_id: source_instance_id.to_owned(),
                    observation_count: 0,
                    producer_ids: Vec::new(),
                    credential_ids: Vec::new(),
                    blockers: Vec::new(),
                });
            item.observation_count = item.observation_count.saturating_add(1);
            for key in CUTOVER_PRODUCER_META_KEYS {
                if let Some(value) = meta.get(*key).and_then(serde_json::Value::as_str)
                    && !value.trim().is_empty()
                    && !item.producer_ids.iter().any(|id| id == value)
                {
                    item.producer_ids.push(value.to_owned());
                }
            }
            for key in CUTOVER_CREDENTIAL_META_KEYS {
                if let Some(value) = meta.get(*key).and_then(serde_json::Value::as_str)
                    && !value.trim().is_empty()
                    && !item.credential_ids.iter().any(|id| id == value)
                {
                    item.credential_ids.push(value.to_owned());
                }
            }
            if let Some(declared) = meta
                .get("source_instance_id")
                .and_then(serde_json::Value::as_str)
                && declared != source_instance_id
            {
                item.blockers.push(format!(
                    "source_instance_id rename detected: {} -> {}",
                    declared, source_instance_id
                ));
            }
        }
        for item in units.values_mut() {
            item.producer_ids.sort();
            item.credential_ids.sort();
            item.blockers.dedup();
            if item.producer_ids.len() > 1 {
                item.blockers.push(
                    "multiple producers share this cutover unit; drain as one unit".to_owned(),
                );
            }
            if item.credential_ids.len() > 1 {
                item.blockers.push(
                    "credential is shared across producers; separate it before cutover".to_owned(),
                );
            } else if item.producer_ids.len() > 1 && item.credential_ids.len() == 1 {
                item.blockers.push(
                    "one credential reference is shared by multiple producers; separate it before cutover"
                        .to_owned(),
                );
            }
        }
        let mut statement = self
            .conn
            .prepare("SELECT DISTINCT source_instance_id FROM cutover_transition_log ORDER BY source_instance_id")
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let registered = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(PersistenceError::from)
            .map_err(storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        for source_instance_id in registered {
            units
                .entry(source_instance_id.clone())
                .or_insert(CutoverInventoryItem {
                    source_instance_id,
                    observation_count: 0,
                    producer_ids: Vec::new(),
                    credential_ids: Vec::new(),
                    blockers: Vec::new(),
                });
        }
        Ok(units.into_values().collect())
    }

    fn cutover_begin_drain(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let state =
            required_state_transaction(&transaction, source_instance_id).map_err(storage_error)?;
        if state.phase != CutoverPhase::V1Active {
            return Err(StorageError::CutoverConflict(format!(
                "unit is in {}, only v1_active may enter draining",
                state.phase.as_str()
            )));
        }
        let fence_append_seq = transaction
            .query_row(
                "SELECT COALESCE(MAX(append_seq), 0) FROM observations",
                [],
                |row| row.get::<_, u64>(0),
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        record_transition(
            &transaction,
            source_instance_id,
            state.phase.as_str(),
            CutoverPhase::Draining,
            authority,
            reason,
            state.generation,
            Some(fence_append_seq),
            state.first_v2_append_seq,
        )
        .map_err(storage_error)?;
        transaction
            .execute(
                "UPDATE cutover_credentials SET active = 0 WHERE source_instance_id = ?1",
                [source_instance_id],
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        self.cutover_state(source_instance_id)
    }

    fn cutover_readiness(
        &self,
        source_instance_id: &str,
        fixture: Option<&CutoverFixture>,
    ) -> StorageResult<CutoverReadinessReport> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let report = readiness_transaction(&transaction, source_instance_id, fixture)
            .map_err(storage_error)?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        Ok(report)
    }

    fn cutover_activate(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
        fixture: &CutoverFixture,
    ) -> StorageResult<CutoverState> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let report = readiness_transaction(&transaction, source_instance_id, Some(fixture))
            .map_err(storage_error)?;
        if !report.ready {
            return Err(StorageError::CutoverConflict(format!(
                "cutover activation blocked: {}",
                report
                    .blockers
                    .iter()
                    .map(|blocker| blocker.reason.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        let state = report.state;
        let generation = state.generation.checked_add(1).ok_or_else(|| {
            StorageError::CutoverConflict("cutover credential generation overflow".to_owned())
        })?;
        record_transition(
            &transaction,
            source_instance_id,
            state.phase.as_str(),
            CutoverPhase::V2Active,
            authority,
            reason,
            generation,
            state.fence_append_seq,
            state.first_v2_append_seq,
        )
        .map_err(storage_error)?;
        set_active_credential(
            &transaction,
            source_instance_id,
            CutoverApiVersion::V2,
            generation,
        )
        .map_err(storage_error)?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        self.cutover_state(source_instance_id)
    }

    fn cutover_rollback(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState> {
        validate_cutover_unit(source_instance_id).map_err(storage_error)?;
        let transaction = self
            .conn
            .unchecked_transaction()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let state =
            required_state_transaction(&transaction, source_instance_id).map_err(storage_error)?;
        if state.phase == CutoverPhase::V2Committed || state.v2_ingested > 0 {
            return Err(StorageError::CutoverRollbackRefused(
                "rollback refused after first v2 ingested observation; forward-fix is required"
                    .to_owned(),
            ));
        }
        if !matches!(state.phase, CutoverPhase::Draining | CutoverPhase::V2Active) {
            return Err(StorageError::CutoverConflict(format!(
                "rollback is not valid from {}",
                state.phase.as_str()
            )));
        }
        let generation = state.generation.checked_add(1).ok_or_else(|| {
            StorageError::CutoverConflict("cutover credential generation overflow".to_owned())
        })?;
        record_transition(
            &transaction,
            source_instance_id,
            state.phase.as_str(),
            CutoverPhase::V1Active,
            authority,
            reason,
            generation,
            state.fence_append_seq,
            state.first_v2_append_seq,
        )
        .map_err(storage_error)?;
        set_active_credential(
            &transaction,
            source_instance_id,
            CutoverApiVersion::V1,
            generation,
        )
        .map_err(storage_error)?;
        transaction
            .commit()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        self.cutover_state(source_instance_id)
    }

    fn cutover_health(&self, source_instance_id: &str) -> StorageResult<CutoverHealth> {
        let state = self.cutover_state(source_instance_id)?;
        let watermark = self.identity_bridge_watermark()?;
        let fence = state.fence_append_seq.unwrap_or(watermark);
        let candidate_count = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM identity_bridge_candidates WHERE source_instance_id = ?1",
                [source_instance_id],
                |row| row.get::<_, u64>(0),
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let gap_count = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM identity_bridge_gaps
                 WHERE source_instance_id = ?1 OR source_instance_id IS NULL",
                [source_instance_id],
                |row| row.get::<_, u64>(0),
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let multiplicity_count = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM (
                     SELECT v2_identity_key FROM identity_bridge_candidates
                     WHERE source_instance_id = ?1 GROUP BY v2_identity_key HAVING COUNT(*) > 1
                 )",
                [source_instance_id],
                |row| row.get::<_, u64>(0),
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let collision_count = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM (
                     SELECT v2_identity_key FROM identity_bridge_candidates
                     WHERE source_instance_id = ?1 GROUP BY v2_identity_key
                     HAVING COUNT(DISTINCT canonical_json) > 1
                 )",
                [source_instance_id],
                |row| row.get::<_, u64>(0),
            )
            .map_err(PersistenceError::from)
            .map_err(storage_error)?;
        let metrics = self
            .conn
            .query_row(
                "SELECT bridge_duplicate_hits, stale_v1_rejections
                 FROM cutover_unit_metrics WHERE source_instance_id = ?1",
                [source_instance_id],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()
            .map_err(PersistenceError::from)
            .map_err(storage_error)?
            .unwrap_or((0, 0));
        Ok(CutoverHealth {
            state,
            bridge_watermark: watermark,
            bridge_lag: fence.saturating_sub(watermark),
            candidate_count,
            gap_count,
            multiplicity_count,
            collision_count,
            bridge_duplicate_hit_count: metrics.0,
            stale_v1_rejection_count: metrics.1,
        })
    }
}
