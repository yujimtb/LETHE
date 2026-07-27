use std::collections::BTreeMap;

use lethe_core::domain::{Observation, ObservationId};
use lethe_storage_api::{
    AppendOutcome, AuditEventRecord, CutoverApiVersion, CutoverBlocker, CutoverFixture,
    CutoverHealth, CutoverInventoryItem, CutoverPhase, CutoverReadinessReport, CutoverState,
    CutoverStore, IdentityBridgeBatchReport, IdentityBridgeResolution, SlackThreadKey,
    StorageError, StorageResult,
};
use postgres::Transaction;
use sha2::{Digest, Sha256};

use super::PostgresPersistence;
use super::general::{
    append_observations_transaction, insert_audit_events, lock_partition_tree,
    observation_blob_refs, validate_append_inputs,
};
use super::general_s3::lock_blob_admission;
use super::general_slack::{upsert_thread, validate_thread_key};

const CANONICAL_JSON_META_KEY: &str = "canonical_json";
const SOURCE_INSTANCE_META_KEY: &str = "source_instance";
const OBJECT_ID_META_KEY: &str = "object_id";
const PRODUCER_ID_META_KEY: &str = "producer_id";
const CREDENTIAL_ID_META_KEY: &str = "credential_id";

impl CutoverStore for PostgresPersistence {
    fn append_observations_v1_with_admission(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observations: &[Observation],
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<Vec<AppendOutcome>> {
        validate_unit(source_instance_id)?;
        validate_append_inputs(observations, audit_events)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&observation_blob_refs(observations))?;
        if let Some(reason) = admission_denial(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V1,
            generation,
        )? {
            record_stale_v1(&mut transaction, source_instance_id)?;
            transaction.commit().map_err(backend)?;
            return Err(StorageError::CutoverAdmissionDenied(reason));
        }
        lock_partition_tree(&mut transaction)?;
        let outcomes = append_observations_transaction(&mut transaction, observations)?;
        insert_audit_events(&mut transaction, audit_events)?;
        transaction.commit().map_err(backend)?;
        Ok(outcomes)
    }

    fn append_slack_observation_v1_with_admission(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observation: &Observation,
        thread: &SlackThreadKey,
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<AppendOutcome> {
        validate_unit(source_instance_id)?;
        validate_thread_key(thread)?;
        validate_append_inputs(std::slice::from_ref(observation), audit_events)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&observation_blob_refs(std::slice::from_ref(
            observation,
        )))?;
        if let Some(reason) = admission_denial(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V1,
            generation,
        )? {
            record_stale_v1(&mut transaction, source_instance_id)?;
            transaction.commit().map_err(backend)?;
            return Err(StorageError::CutoverAdmissionDenied(reason));
        }
        lock_partition_tree(&mut transaction)?;
        let outcome =
            append_observations_transaction(&mut transaction, std::slice::from_ref(observation))?
                .pop()
                .ok_or_else(|| {
                    StorageError::Invariant("v1 Slack append returned no outcome".to_owned())
                })?;
        if let AppendOutcome::Appended(id) | AppendOutcome::Duplicate(id) = &outcome {
            let append_seq: i64 = transaction
                .query_one(
                    "SELECT append_seq FROM observations WHERE observation_id = $1",
                    &[&id.as_str()],
                )
                .map_err(backend)?
                .get(0);
            upsert_thread(&mut transaction, thread, append_seq)?;
        }
        insert_audit_events(&mut transaction, audit_events)?;
        transaction.commit().map_err(backend)?;
        Ok(outcome)
    }

    fn append_observations_v2_with_bridge(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        observations: &[Observation],
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<Vec<AppendOutcome>> {
        validate_unit(source_instance_id)?;
        validate_append_inputs(observations, audit_events)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&observation_blob_refs(observations))?;
        if let Some(reason) = admission_denial(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V2,
            generation,
        )? {
            transaction.commit().map_err(backend)?;
            return Err(StorageError::CutoverAdmissionDenied(reason));
        }
        lock_partition_tree(&mut transaction)?;
        let mut outcomes = Vec::with_capacity(observations.len());
        let mut bridge_hits = 0_u64;
        let mut appended_ids = Vec::new();
        for observation in observations {
            let (observed_source, object_id, canonical_json) =
                observation_identity_inputs(observation).map_err(StorageError::Invariant)?;
            if observed_source != source_instance_id {
                return Err(StorageError::Invariant(format!(
                    "v2 observation {} belongs to source_instance {}, expected {source_instance_id}",
                    observation.id, observed_source
                )));
            }
            let identity = bridge_identity_key(source_instance_id, &object_id, &canonical_json);
            if observation.idempotency_key.as_str() != identity {
                return Err(StorageError::Invariant(format!(
                    "v2 observation {} identity does not match bridge formula",
                    observation.id
                )));
            }
            let resolution = bridge_resolution(&mut transaction, &identity, &canonical_json)?;
            let outcome = if let Some(winner) = resolution.winner {
                if resolution.canonical_collision {
                    AppendOutcome::CanonicalCollision(winner)
                } else {
                    bridge_hits = bridge_hits.checked_add(1).ok_or_else(|| {
                        StorageError::Invariant("bridge duplicate metric overflow".to_owned())
                    })?;
                    AppendOutcome::Duplicate(winner)
                }
            } else {
                let outcome = append_observations_transaction(
                    &mut transaction,
                    std::slice::from_ref(observation),
                )?
                .pop()
                .ok_or_else(|| {
                    StorageError::Invariant("v2 append returned no outcome".to_owned())
                })?;
                if let AppendOutcome::Appended(id) = &outcome {
                    appended_ids.push(id.clone());
                }
                outcome
            };
            outcomes.push(outcome);
        }
        insert_audit_events(&mut transaction, audit_events)?;
        record_v2_metrics_and_commit(
            &mut transaction,
            source_instance_id,
            bridge_hits,
            &appended_ids,
        )?;
        transaction.commit().map_err(backend)?;
        Ok(outcomes)
    }

    fn append_observations_v2_atomic_page(
        &self,
        source_instance_id: &str,
        generation: u64,
        observations: &[Observation],
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<Vec<AppendOutcome>> {
        validate_unit(source_instance_id)?;
        if generation == 0 {
            return Err(StorageError::Invariant(
                "atomic page admission generation must be positive".to_owned(),
            ));
        }
        validate_append_inputs(observations, audit_events)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&observation_blob_refs(observations))?;
        if optional_state(&mut transaction, source_instance_id, true)?.is_none() {
            return Err(StorageError::CutoverAdmissionDenied(format!(
                "atomic page cutover unit {source_instance_id} is not registered"
            )));
        }
        if let Some(reason) = admission_denial(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V2,
            Some(generation),
        )? {
            return Err(StorageError::CutoverAdmissionDenied(reason));
        }
        lock_partition_tree(&mut transaction)?;
        let mut outcomes = Vec::with_capacity(observations.len());
        let mut bridge_hits = 0_u64;
        let mut appended_ids = Vec::new();
        for (index, observation) in observations.iter().enumerate() {
            let (observed_source, object_id, canonical_json) =
                observation_identity_inputs(observation).map_err(StorageError::Invariant)?;
            if observed_source != source_instance_id {
                return Err(StorageError::Invariant(format!(
                    "v2 observation {} belongs to source_instance {}, expected {source_instance_id}",
                    observation.id, observed_source
                )));
            }
            let identity = bridge_identity_key(source_instance_id, &object_id, &canonical_json);
            if observation.idempotency_key.as_str() != identity {
                return Err(StorageError::Invariant(format!(
                    "v2 observation {} identity does not match bridge formula",
                    observation.id
                )));
            }
            let resolution = bridge_resolution(&mut transaction, &identity, &canonical_json)?;
            let outcome = if let Some(winner) = resolution.winner {
                if resolution.canonical_collision {
                    return Err(StorageError::AtomicPageCollision {
                        index,
                        existing_id: winner,
                    });
                }
                bridge_hits = bridge_hits.checked_add(1).ok_or_else(|| {
                    StorageError::Invariant("bridge duplicate metric overflow".to_owned())
                })?;
                AppendOutcome::Duplicate(winner)
            } else {
                let outcome = append_observations_transaction(
                    &mut transaction,
                    std::slice::from_ref(observation),
                )?
                .pop()
                .ok_or_else(|| {
                    StorageError::Invariant("v2 atomic append returned no outcome".to_owned())
                })?;
                match &outcome {
                    AppendOutcome::Appended(id) => appended_ids.push(id.clone()),
                    AppendOutcome::Duplicate(_) => {}
                    AppendOutcome::CanonicalCollision(existing_id) => {
                        return Err(StorageError::AtomicPageCollision {
                            index,
                            existing_id: existing_id.clone(),
                        });
                    }
                }
                outcome
            };
            outcomes.push(outcome);
        }
        insert_audit_events(&mut transaction, audit_events)?;
        record_v2_metrics_and_commit(
            &mut transaction,
            source_instance_id,
            bridge_hits,
            &appended_ids,
        )?;
        transaction.commit().map_err(backend)?;
        Ok(outcomes)
    }

    fn cutover_admit(
        &self,
        source_instance_id: &str,
        api_version: CutoverApiVersion,
        generation: Option<u64>,
    ) -> StorageResult<()> {
        validate_unit(source_instance_id)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        if let Some(reason) = admission_denial(
            &mut transaction,
            source_instance_id,
            api_version,
            generation,
        )? {
            if api_version == CutoverApiVersion::V1 {
                record_stale_v1(&mut transaction, source_instance_id)?;
            }
            transaction.commit().map_err(backend)?;
            return Err(StorageError::CutoverAdmissionDenied(reason));
        }
        transaction.commit().map_err(backend)
    }

    fn identity_bridge_apply_batch(
        &self,
        batch_size: usize,
    ) -> StorageResult<IdentityBridgeBatchReport> {
        positive("identity bridge batch size", batch_size)?;
        let batch_size = usize_to_i64("identity bridge batch size", batch_size)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        let previous: i64 = transaction
            .query_one(
                "SELECT append_seq FROM identity_bridge_watermark
                 WHERE singleton FOR UPDATE",
                &[],
            )
            .map_err(backend)?
            .get(0);
        let rows = transaction
            .query(
                "SELECT append_seq, observation_id, observation_json::text
                 FROM observations
                 WHERE append_seq > $1
                 ORDER BY append_seq
                 LIMIT $2",
                &[&previous, &batch_size],
            )
            .map_err(backend)?;
        if rows.is_empty() {
            transaction.commit().map_err(backend)?;
            let previous = from_i64("identity bridge watermark", previous)?;
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
        for row in &rows {
            let append_seq: i64 = row.get(0);
            let observation_id: String = row.get(1);
            let observation_json: String = row.get(2);
            let observation: Observation =
                serde_json::from_str(&observation_json).map_err(|error| {
                    StorageError::Invariant(format!(
                        "identity bridge observation JSON is invalid: {error}"
                    ))
                })?;
            match observation_identity_inputs(&observation) {
                Ok((source_instance_id, object_id, canonical_json)) => {
                    let identity =
                        bridge_identity_key(&source_instance_id, &object_id, &canonical_json);
                    if observation.idempotency_key.as_str() != identity {
                        candidate_count += usize::try_from(
                            transaction
                                .execute(
                                    "INSERT INTO identity_bridge_candidates (
                                        v2_identity_key, observation_id,
                                        source_instance_id, append_seq,
                                        canonical_json, canonical_json_sha256
                                     ) VALUES ($1, $2, $3, $4, $5, $6)
                                     ON CONFLICT DO NOTHING",
                                    &[
                                        &identity,
                                        &observation_id,
                                        &source_instance_id,
                                        &append_seq,
                                        &canonical_json,
                                        &sha256_hex(canonical_json.as_bytes()),
                                    ],
                                )
                                .map_err(backend)?,
                        )
                        .map_err(|_| {
                            StorageError::Invariant(
                                "identity bridge candidate count exceeds usize".to_owned(),
                            )
                        })?;
                    }
                }
                Err(reason) => {
                    let source_instance = observation
                        .meta
                        .as_object()
                        .and_then(|meta| meta.get(SOURCE_INSTANCE_META_KEY))
                        .and_then(serde_json::Value::as_str);
                    gap_count += usize::try_from(
                        transaction
                            .execute(
                                "INSERT INTO identity_bridge_gaps (
                                    append_seq, observation_id,
                                    source_instance_id, reason
                                 ) VALUES ($1, $2, $3, $4)
                                 ON CONFLICT DO NOTHING",
                                &[&append_seq, &observation_id, &source_instance, &reason],
                            )
                            .map_err(backend)?,
                    )
                    .map_err(|_| {
                        StorageError::Invariant(
                            "identity bridge gap count exceeds usize".to_owned(),
                        )
                    })?;
                }
            }
            last_append_seq = append_seq;
        }
        transaction
            .execute(
                "UPDATE identity_bridge_watermark
                 SET append_seq = $1 WHERE singleton",
                &[&last_append_seq],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)?;
        Ok(IdentityBridgeBatchReport {
            previous_watermark: from_i64("previous identity bridge watermark", previous)?,
            watermark: from_i64("identity bridge watermark", last_append_seq)?,
            read_count: rows.len(),
            candidate_count,
            gap_count,
        })
    }

    fn identity_bridge_watermark(&self) -> StorageResult<u64> {
        let mut reader = self.reader()?;
        from_i64(
            "identity bridge watermark",
            reader
                .query_one(
                    "SELECT append_seq FROM identity_bridge_watermark WHERE singleton",
                    &[],
                )
                .map_err(backend)?
                .get(0),
        )
    }

    fn identity_bridge_resolve(
        &self,
        v2_identity_key: &str,
        canonical_json: &str,
    ) -> StorageResult<IdentityBridgeResolution> {
        non_blank("v2 identity key", v2_identity_key)?;
        validate_canonical_json(canonical_json)?;
        let mut reader = self.reader()?;
        bridge_resolution(&mut *reader, v2_identity_key, canonical_json)
    }

    fn cutover_register(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState> {
        validate_unit(source_instance_id)?;
        validate_transition_text(authority, reason)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        if let Some(state) = optional_state(&mut transaction, source_instance_id, true)? {
            transaction.commit().map_err(backend)?;
            return Ok(state);
        }
        transaction
            .execute(
                "INSERT INTO cutover_states (
                    source_instance_id, authority, phase, generation
                 ) VALUES ($1, $2, 'v1_active', 1)",
                &[&source_instance_id, &authority],
            )
            .map_err(backend)?;
        insert_transition(
            &mut transaction,
            source_instance_id,
            "uninitialized",
            CutoverPhase::V1Active,
            authority,
            reason,
            1,
            None,
            None,
        )?;
        set_active_credential(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V1,
            1,
        )?;
        ensure_metrics(&mut transaction, source_instance_id)?;
        let state = required_state(&mut transaction, source_instance_id, false)?;
        transaction.commit().map_err(backend)?;
        Ok(state)
    }

    fn bootstrap_v3_source_unit(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState> {
        validate_unit(source_instance_id)?;
        validate_transition_text(authority, reason)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&source_instance_id],
            )
            .map_err(backend)?;
        let existing_observation: bool = transaction
            .query_one(
                "SELECT EXISTS (
                    SELECT 1
                    FROM observations
                    WHERE observation_json #>> '{meta,source_instance}' = $1
                 )",
                &[&source_instance_id],
            )
            .map_err(backend)?
            .get(0);
        let existing_state = optional_state(&mut transaction, source_instance_id, true)?;
        if existing_observation {
            return Err(StorageError::CutoverConflict(format!(
                "v3 source unit {source_instance_id} already has canonical Observations"
            )));
        }
        if let Some(state) = existing_state {
            if state.phase == CutoverPhase::V2Active
                && state.generation == 1
                && state.fence_append_seq.is_none()
                && state.first_v2_append_seq.is_none()
                && state.v2_ingested == 0
            {
                transaction.commit().map_err(backend)?;
                return Ok(state);
            }
            return Err(StorageError::CutoverConflict(format!(
                "v3 source unit {source_instance_id} already has a non-bootstrap state"
            )));
        }
        transaction
            .execute(
                "INSERT INTO cutover_states (
                    source_instance_id, authority, phase, generation
                 ) VALUES ($1, $2, 'v2_active', 1)",
                &[&source_instance_id, &authority],
            )
            .map_err(backend)?;
        insert_transition(
            &mut transaction,
            source_instance_id,
            "uninitialized",
            CutoverPhase::V2Active,
            authority,
            reason,
            1,
            None,
            None,
        )?;
        set_active_credential(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V2,
            1,
        )?;
        ensure_metrics(&mut transaction, source_instance_id)?;
        let state = required_state(&mut transaction, source_instance_id, false)?;
        transaction.commit().map_err(backend)?;
        Ok(state)
    }

    fn cutover_state(&self, source_instance_id: &str) -> StorageResult<CutoverState> {
        validate_unit(source_instance_id)?;
        let mut reader = self.reader()?;
        required_state(&mut *reader, source_instance_id, false)
    }

    fn cutover_inventory(&self) -> StorageResult<Vec<CutoverInventoryItem>> {
        let mut reader = self.reader()?;
        let mut units = BTreeMap::<String, CutoverInventoryItem>::new();
        for row in reader
            .query(
                "SELECT observation_json::text FROM observations ORDER BY append_seq",
                &[],
            )
            .map_err(backend)?
        {
            let json: String = row.get(0);
            let observation: Observation = serde_json::from_str(&json).map_err(|error| {
                StorageError::Invariant(format!(
                    "cutover inventory observation JSON is invalid: {error}"
                ))
            })?;
            let Some(meta) = observation.meta.as_object() else {
                continue;
            };
            let Some(source_instance_id) = meta
                .get(SOURCE_INSTANCE_META_KEY)
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
            item.observation_count = item.observation_count.checked_add(1).ok_or_else(|| {
                StorageError::Invariant("cutover inventory count overflow".to_owned())
            })?;
            insert_metadata_value(meta, PRODUCER_ID_META_KEY, &mut item.producer_ids);
            insert_metadata_value(meta, CREDENTIAL_ID_META_KEY, &mut item.credential_ids);
        }
        for row in reader
            .query(
                "SELECT source_instance_id FROM cutover_states
                 ORDER BY source_instance_id",
                &[],
            )
            .map_err(backend)?
        {
            let source_instance_id: String = row.get(0);
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
        for item in units.values_mut() {
            item.producer_ids.sort();
            item.credential_ids.sort();
            if item.producer_ids.len() > 1 {
                item.blockers.push(
                    "multiple producers share this cutover unit; drain as one unit".to_owned(),
                );
            }
            if item.credential_ids.len() > 1 {
                item.blockers.push(
                    "multiple credentials share this cutover unit; separate them before cutover"
                        .to_owned(),
                );
            }
        }
        Ok(units.into_values().collect())
    }

    fn cutover_begin_drain(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState> {
        validate_unit(source_instance_id)?;
        validate_transition_text(authority, reason)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        let state = required_state(&mut transaction, source_instance_id, true)?;
        if state.phase != CutoverPhase::V1Active {
            return Err(StorageError::CutoverConflict(format!(
                "unit is in {}, only v1_active may enter draining",
                state.phase.as_str()
            )));
        }
        let fence: i64 = transaction
            .query_one("SELECT COALESCE(MAX(append_seq), 0) FROM observations", &[])
            .map_err(backend)?
            .get(0);
        transition_state(
            &mut transaction,
            &state,
            CutoverPhase::Draining,
            state.generation,
            Some(from_i64("cutover fence", fence)?),
            state.first_v2_append_seq,
            state.v2_ingested,
            authority,
            reason,
        )?;
        deactivate_credentials(&mut transaction, source_instance_id)?;
        let result = required_state(&mut transaction, source_instance_id, false)?;
        transaction.commit().map_err(backend)?;
        Ok(result)
    }

    fn cutover_readiness(
        &self,
        source_instance_id: &str,
        fixture: Option<&CutoverFixture>,
    ) -> StorageResult<CutoverReadinessReport> {
        validate_unit(source_instance_id)?;
        let mut reader = self.reader()?;
        readiness(&mut *reader, source_instance_id, fixture)
    }

    fn cutover_activate(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
        fixture: &CutoverFixture,
    ) -> StorageResult<CutoverState> {
        validate_unit(source_instance_id)?;
        validate_transition_text(authority, reason)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        let report = readiness(&mut transaction, source_instance_id, Some(fixture))?;
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
            StorageError::CutoverConflict("cutover generation overflow".to_owned())
        })?;
        transition_state(
            &mut transaction,
            &state,
            CutoverPhase::V2Active,
            generation,
            state.fence_append_seq,
            state.first_v2_append_seq,
            state.v2_ingested,
            authority,
            reason,
        )?;
        set_active_credential(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V2,
            generation,
        )?;
        let result = required_state(&mut transaction, source_instance_id, false)?;
        transaction.commit().map_err(backend)?;
        Ok(result)
    }

    fn cutover_rollback(
        &self,
        source_instance_id: &str,
        authority: &str,
        reason: &str,
    ) -> StorageResult<CutoverState> {
        validate_unit(source_instance_id)?;
        validate_transition_text(authority, reason)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        let state = required_state(&mut transaction, source_instance_id, true)?;
        let initial_phase: String = transaction
            .query_one(
                "SELECT to_phase
                 FROM cutover_transition_log
                 WHERE source_instance_id = $1
                 ORDER BY transition_seq
                 LIMIT 1",
                &[&source_instance_id],
            )
            .map_err(backend)?
            .get(0);
        if initial_phase == CutoverPhase::V2Active.as_str() {
            return Err(StorageError::CutoverRollbackRefused(
                "rollback refused for a v3-native source unit with no v1 protocol".to_owned(),
            ));
        }
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
            StorageError::CutoverConflict("cutover generation overflow".to_owned())
        })?;
        transition_state(
            &mut transaction,
            &state,
            CutoverPhase::V1Active,
            generation,
            state.fence_append_seq,
            state.first_v2_append_seq,
            state.v2_ingested,
            authority,
            reason,
        )?;
        set_active_credential(
            &mut transaction,
            source_instance_id,
            CutoverApiVersion::V1,
            generation,
        )?;
        let result = required_state(&mut transaction, source_instance_id, false)?;
        transaction.commit().map_err(backend)?;
        Ok(result)
    }

    fn cutover_health(&self, source_instance_id: &str) -> StorageResult<CutoverHealth> {
        validate_unit(source_instance_id)?;
        let mut reader = self.reader()?;
        let state = required_state(&mut *reader, source_instance_id, false)?;
        let watermark = bridge_watermark(&mut *reader)?;
        let fence = state.fence_append_seq.unwrap_or(watermark);
        let counts = bridge_counts(&mut *reader, source_instance_id)?;
        let metrics = reader
            .query_opt(
                "SELECT bridge_duplicate_hits, stale_v1_rejections
                 FROM cutover_unit_metrics WHERE source_instance_id = $1",
                &[&source_instance_id],
            )
            .map_err(backend)?
            .map_or((0_i64, 0_i64), |row| (row.get(0), row.get(1)));
        Ok(CutoverHealth {
            state,
            bridge_watermark: watermark,
            bridge_lag: fence.saturating_sub(watermark),
            candidate_count: counts.candidates,
            gap_count: counts.gaps,
            multiplicity_count: counts.multiplicities,
            collision_count: counts.collisions,
            bridge_duplicate_hit_count: from_i64("bridge duplicate hits", metrics.0)?,
            stale_v1_rejection_count: from_i64("stale v1 rejections", metrics.1)?,
        })
    }
}

fn optional_state(
    client: &mut impl postgres::GenericClient,
    source_instance_id: &str,
    for_update: bool,
) -> StorageResult<Option<CutoverState>> {
    let suffix = if for_update { " FOR UPDATE" } else { "" };
    let row = client
        .query_opt(
            &format!(
                "SELECT phase, generation, fence_append_seq,
                        first_v2_append_seq, v2_ingested
                 FROM cutover_states WHERE source_instance_id = $1{suffix}"
            ),
            &[&source_instance_id],
        )
        .map_err(backend)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let state = CutoverState {
        source_instance_id: source_instance_id.to_owned(),
        phase: parse_phase(row.get::<_, String>(0).as_str())?,
        generation: from_i64("cutover generation", row.get(1))?,
        fence_append_seq: optional_u64("cutover fence", row.get(2))?,
        first_v2_append_seq: optional_u64("first v2 append sequence", row.get(3))?,
        v2_ingested: from_i64("v2 ingested count", row.get(4))?,
    };
    validate_transition_history(client, &state)?;
    Ok(Some(state))
}

fn required_state(
    client: &mut impl postgres::GenericClient,
    source_instance_id: &str,
    for_update: bool,
) -> StorageResult<CutoverState> {
    optional_state(client, source_instance_id, for_update)?.ok_or_else(|| {
        StorageError::Invariant(format!(
            "cutover unit {source_instance_id} is not registered"
        ))
    })
}

fn validate_transition_history(
    client: &mut impl postgres::GenericClient,
    stored: &CutoverState,
) -> StorageResult<()> {
    let rows = client
        .query(
            "SELECT from_phase, to_phase, generation,
                    fence_append_seq, first_v2_append_seq
             FROM cutover_transition_log
             WHERE source_instance_id = $1
             ORDER BY transition_seq",
            &[&stored.source_instance_id],
        )
        .map_err(backend)?;
    let mut phase = None;
    let mut generation = 0_u64;
    let mut fence = None;
    let mut first_v2 = None;
    for row in rows {
        let from: String = row.get(0);
        let to = parse_phase(row.get::<_, String>(1).as_str())?;
        let next_generation = from_i64("transition generation", row.get(2))?;
        let next_fence = optional_u64("transition fence", row.get(3))?;
        let next_first = optional_u64("transition first v2 append", row.get(4))?;
        let valid = match phase {
            None => {
                from == "uninitialized"
                    && matches!(to, CutoverPhase::V1Active | CutoverPhase::V2Active)
                    && next_generation == 1
            }
            Some(CutoverPhase::V1Active) => {
                from == CutoverPhase::V1Active.as_str()
                    && to == CutoverPhase::Draining
                    && next_generation == generation
            }
            Some(CutoverPhase::Draining) => {
                from == CutoverPhase::Draining.as_str()
                    && matches!(to, CutoverPhase::V2Active | CutoverPhase::V1Active)
                    && next_generation == generation.saturating_add(1)
            }
            Some(CutoverPhase::V2Active) => {
                from == CutoverPhase::V2Active.as_str()
                    && ((to == CutoverPhase::V2Committed
                        && next_generation == generation
                        && next_first.is_some())
                        || (to == CutoverPhase::V1Active
                            && next_generation == generation.saturating_add(1)))
            }
            Some(CutoverPhase::V2Committed) => false,
        };
        if !valid {
            return Err(StorageError::Invariant(format!(
                "invalid cutover transition {from} -> {} for {}",
                to.as_str(),
                stored.source_instance_id
            )));
        }
        phase = Some(to);
        generation = next_generation;
        if next_fence.is_some() {
            fence = next_fence;
        }
        if next_first.is_some() {
            first_v2 = next_first;
        }
    }
    if phase != Some(stored.phase)
        || generation != stored.generation
        || fence != stored.fence_append_seq
        || first_v2 != stored.first_v2_append_seq
    {
        return Err(StorageError::Invariant(format!(
            "cutover state and transition history disagree for {}",
            stored.source_instance_id
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn transition_state(
    transaction: &mut Transaction<'_>,
    previous: &CutoverState,
    next_phase: CutoverPhase,
    next_generation: u64,
    fence_append_seq: Option<u64>,
    first_v2_append_seq: Option<u64>,
    v2_ingested: u64,
    authority: &str,
    reason: &str,
) -> StorageResult<()> {
    validate_transition_text(authority, reason)?;
    insert_transition(
        transaction,
        &previous.source_instance_id,
        previous.phase.as_str(),
        next_phase,
        authority,
        reason,
        next_generation,
        fence_append_seq,
        first_v2_append_seq,
    )?;
    let changed = transaction
        .execute(
            "UPDATE cutover_states
             SET authority = $1, phase = $2, generation = $3,
                 fence_append_seq = $4, first_v2_append_seq = $5,
                 v2_ingested = $6, updated_at = clock_timestamp()
             WHERE source_instance_id = $7
               AND phase = $8 AND generation = $9",
            &[
                &authority,
                &next_phase.as_str(),
                &to_i64("cutover generation", next_generation)?,
                &optional_i64("cutover fence", fence_append_seq)?,
                &optional_i64("first v2 append sequence", first_v2_append_seq)?,
                &to_i64("v2 ingested count", v2_ingested)?,
                &previous.source_instance_id,
                &previous.phase.as_str(),
                &to_i64("previous cutover generation", previous.generation)?,
            ],
        )
        .map_err(backend)?;
    if changed != 1 {
        return Err(StorageError::CutoverConflict(format!(
            "cutover state changed concurrently for {}",
            previous.source_instance_id
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_transition(
    transaction: &mut Transaction<'_>,
    source_instance_id: &str,
    from_phase: &str,
    to_phase: CutoverPhase,
    authority: &str,
    reason: &str,
    generation: u64,
    fence_append_seq: Option<u64>,
    first_v2_append_seq: Option<u64>,
) -> StorageResult<()> {
    validate_transition_text(authority, reason)?;
    transaction
        .execute(
            "INSERT INTO cutover_transition_log (
                source_instance_id, authority, reason, from_phase,
                to_phase, generation, fence_append_seq, first_v2_append_seq
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &source_instance_id,
                &authority,
                &reason,
                &from_phase,
                &to_phase.as_str(),
                &to_i64("transition generation", generation)?,
                &optional_i64("transition fence", fence_append_seq)?,
                &optional_i64("transition first v2 append", first_v2_append_seq)?,
            ],
        )
        .map_err(backend)?;
    Ok(())
}

fn admission_denial(
    transaction: &mut Transaction<'_>,
    source_instance_id: &str,
    api_version: CutoverApiVersion,
    generation: Option<u64>,
) -> StorageResult<Option<String>> {
    let Some(state) = optional_state(transaction, source_instance_id, true)? else {
        return Ok(None);
    };
    let correct_phase = match api_version {
        CutoverApiVersion::V1 => state.phase == CutoverPhase::V1Active,
        CutoverApiVersion::V2 => {
            matches!(
                state.phase,
                CutoverPhase::V2Active | CutoverPhase::V2Committed
            )
        }
    };
    if !correct_phase {
        return Ok(Some(format!(
            "unit {source_instance_id} is {}, not admitting {}",
            state.phase.as_str(),
            api_version.as_str()
        )));
    }
    if generation != Some(state.generation) {
        return Ok(Some(format!(
            "{} credential generation is stale or missing for unit {source_instance_id}: expected {}, got {generation:?}",
            api_version.as_str(),
            state.generation
        )));
    }
    let active: bool = transaction
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM cutover_credentials
                WHERE source_instance_id = $1
                  AND api_version = $2
                  AND generation = $3
                  AND active
             )",
            &[
                &source_instance_id,
                &api_version.as_str(),
                &to_i64("credential generation", state.generation)?,
            ],
        )
        .map_err(backend)?
        .get(0);
    if active {
        Ok(None)
    } else {
        Ok(Some(format!(
            "{} credential generation {} is not active for unit {source_instance_id}",
            api_version.as_str(),
            state.generation
        )))
    }
}

fn record_v2_metrics_and_commit(
    transaction: &mut Transaction<'_>,
    source_instance_id: &str,
    bridge_hits: u64,
    appended_ids: &[ObservationId],
) -> StorageResult<()> {
    let Some(state) = optional_state(transaction, source_instance_id, true)? else {
        return Ok(());
    };
    ensure_metrics(transaction, source_instance_id)?;
    if bridge_hits > 0 {
        transaction
            .execute(
                "UPDATE cutover_unit_metrics
                 SET bridge_duplicate_hits = bridge_duplicate_hits + $1,
                     updated_at = clock_timestamp()
                 WHERE source_instance_id = $2",
                &[
                    &to_i64("bridge duplicate hits", bridge_hits)?,
                    &source_instance_id,
                ],
            )
            .map_err(backend)?;
    }
    if appended_ids.is_empty() {
        return Ok(());
    }
    let appended = u64::try_from(appended_ids.len())
        .map_err(|_| StorageError::Invariant("v2 append count overflow".to_owned()))?;
    let total = state
        .v2_ingested
        .checked_add(appended)
        .ok_or_else(|| StorageError::Invariant("v2 ingested count overflow".to_owned()))?;
    let first = appended_ids
        .iter()
        .map(|id| {
            transaction
                .query_one(
                    "SELECT append_seq FROM observations WHERE observation_id = $1",
                    &[&id.as_str()],
                )
                .map_err(backend)
                .and_then(|row| from_i64("first v2 append sequence", row.get(0)))
        })
        .collect::<StorageResult<Vec<_>>>()?
        .into_iter()
        .min();
    if state.phase == CutoverPhase::V2Active {
        transition_state(
            transaction,
            &state,
            CutoverPhase::V2Committed,
            state.generation,
            state.fence_append_seq,
            first,
            total,
            "actor:self-host",
            "first v2 ingested observation committed",
        )
    } else {
        transaction
            .execute(
                "UPDATE cutover_states
                 SET v2_ingested = $1, updated_at = clock_timestamp()
                 WHERE source_instance_id = $2",
                &[&to_i64("v2 ingested count", total)?, &source_instance_id],
            )
            .map_err(backend)?;
        Ok(())
    }
}

fn readiness(
    client: &mut impl postgres::GenericClient,
    source_instance_id: &str,
    fixture: Option<&CutoverFixture>,
) -> StorageResult<CutoverReadinessReport> {
    let state = required_state(client, source_instance_id, false)?;
    let watermark = bridge_watermark(client)?;
    let fence = state.fence_append_seq.unwrap_or(0);
    let watermark_covered = state.fence_append_seq.is_some() && watermark >= fence;
    let counts = bridge_counts(client, source_instance_id)?;
    let (fixture_identity_stable, dry_run_passed, fixture_blocker) = match fixture {
        Some(fixture) => {
            let canonical_valid = validate_canonical_json(&fixture.canonical_json).is_ok();
            let expected = bridge_identity_key(
                source_instance_id,
                &fixture.object_id,
                &fixture.canonical_json,
            );
            let stable = canonical_valid && fixture.expected_identity_key == expected;
            let resolution = bridge_resolution(
                client,
                &fixture.expected_identity_key,
                &fixture.canonical_json,
            )?;
            let dry_run = stable
                && resolution.winner.is_some()
                && !resolution.canonical_collision
                && fixture
                    .expected_observation_id
                    .as_ref()
                    .is_none_or(|expected| resolution.winner.as_ref() == Some(expected));
            (
                stable,
                dry_run,
                (!dry_run)
                    .then(|| "retry fixture identity or existing-id dry-run failed".to_owned()),
            )
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
    if counts.gaps > 0 {
        blockers.push(CutoverBlocker {
            append_seq: Some(fence),
            reason: format!("{} unresolved identity derivation gap(s)", counts.gaps),
        });
    }
    if counts.collisions > 0 {
        blockers.push(CutoverBlocker {
            append_seq: None,
            reason: format!(
                "{} canonical exact-compare collision group(s)",
                counts.collisions
            ),
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
            reason: format!(
                "unit is {}, activation requires draining",
                state.phase.as_str()
            ),
        });
    }
    Ok(CutoverReadinessReport {
        state,
        bridge_watermark: watermark,
        bridge_lag: fence.saturating_sub(watermark),
        watermark_covered,
        unresolved_gap_count: counts.gaps,
        exact_compare_error_count: counts.collisions,
        fixture_identity_stable,
        dry_run_passed,
        candidate_count: counts.candidates,
        multiplicity_count: counts.multiplicities,
        collision_count: counts.collisions,
        ready: blockers.is_empty(),
        blockers,
    })
}

struct BridgeCounts {
    candidates: u64,
    gaps: u64,
    multiplicities: u64,
    collisions: u64,
}

fn bridge_counts(
    client: &mut impl postgres::GenericClient,
    source_instance_id: &str,
) -> StorageResult<BridgeCounts> {
    let row = client
        .query_one(
            "SELECT
                (SELECT COUNT(*) FROM identity_bridge_candidates
                 WHERE source_instance_id = $1),
                (SELECT COUNT(*) FROM identity_bridge_gaps
                 WHERE source_instance_id = $1 OR source_instance_id IS NULL),
                (SELECT COUNT(*) FROM (
                    SELECT v2_identity_key
                    FROM identity_bridge_candidates
                    WHERE source_instance_id = $1
                    GROUP BY v2_identity_key HAVING COUNT(*) > 1
                 ) multiplicities),
                (SELECT COUNT(*) FROM (
                    SELECT v2_identity_key
                    FROM identity_bridge_candidates
                    WHERE source_instance_id = $1
                    GROUP BY v2_identity_key
                    HAVING COUNT(DISTINCT canonical_json) > 1
                 ) collisions)",
            &[&source_instance_id],
        )
        .map_err(backend)?;
    Ok(BridgeCounts {
        candidates: from_i64("bridge candidate count", row.get(0))?,
        gaps: from_i64("bridge gap count", row.get(1))?,
        multiplicities: from_i64("bridge multiplicity count", row.get(2))?,
        collisions: from_i64("bridge collision count", row.get(3))?,
    })
}

fn bridge_resolution(
    client: &mut impl postgres::GenericClient,
    v2_identity_key: &str,
    canonical_json: &str,
) -> StorageResult<IdentityBridgeResolution> {
    non_blank("v2 identity key", v2_identity_key)?;
    validate_canonical_json(canonical_json)?;
    let candidates = client
        .query(
            "SELECT observation_id, append_seq, canonical_json
             FROM identity_bridge_candidates
             WHERE v2_identity_key = $1
             ORDER BY append_seq, observation_id",
            &[&v2_identity_key],
        )
        .map_err(backend)?
        .into_iter()
        .map(|row| {
            Ok((
                ObservationId::new(row.get::<_, String>(0)),
                from_i64("bridge candidate append sequence", row.get(1))?,
                row.get::<_, String>(2),
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let winner = candidates.first();
    let collision = candidates
        .iter()
        .find(|(_, _, candidate)| candidate != canonical_json);
    Ok(IdentityBridgeResolution {
        v2_identity_key: v2_identity_key.to_owned(),
        winner: winner.map(|(id, _, _)| id.clone()),
        winner_append_seq: winner.map(|(_, append_seq, _)| *append_seq),
        multiplicity: u64::try_from(candidates.len())
            .map_err(|_| StorageError::Invariant("bridge multiplicity overflow".to_owned()))?,
        canonical_collision: collision.is_some(),
        collision_append_seq: collision.map(|(_, append_seq, _)| *append_seq),
    })
}

fn bridge_watermark(client: &mut impl postgres::GenericClient) -> StorageResult<u64> {
    from_i64(
        "identity bridge watermark",
        client
            .query_one(
                "SELECT append_seq FROM identity_bridge_watermark WHERE singleton",
                &[],
            )
            .map_err(backend)?
            .get(0),
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
    validate_canonical_json(canonical_json).map_err(|error| error.to_string())?;
    let source_instance = required_meta(meta, SOURCE_INSTANCE_META_KEY)?;
    let object_id = required_meta(meta, OBJECT_ID_META_KEY)?;
    Ok((source_instance, object_id, canonical_json.to_owned()))
}

fn required_meta(
    meta: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<String, String> {
    meta.get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("meta.{key} is missing or blank"))
}

fn bridge_identity_key(source_instance_id: &str, object_id: &str, canonical_json: &str) -> String {
    format!(
        "{source_instance_id}:{object_id}:{}",
        sha256_hex(canonical_json.as_bytes())
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn parse_phase(value: &str) -> StorageResult<CutoverPhase> {
    match value {
        "v1_active" => Ok(CutoverPhase::V1Active),
        "draining" => Ok(CutoverPhase::Draining),
        "v2_active" => Ok(CutoverPhase::V2Active),
        "v2_committed" => Ok(CutoverPhase::V2Committed),
        _ => Err(StorageError::Invariant(format!(
            "unknown cutover phase {value:?}"
        ))),
    }
}

fn set_active_credential(
    transaction: &mut Transaction<'_>,
    source_instance_id: &str,
    api_version: CutoverApiVersion,
    generation: u64,
) -> StorageResult<()> {
    deactivate_credentials(transaction, source_instance_id)?;
    let credential_id = format!(
        "unit:{source_instance_id}:{}:{generation}",
        api_version.as_str()
    );
    transaction
        .execute(
            "INSERT INTO cutover_credentials (
                source_instance_id, api_version, generation,
                credential_id, active
             ) VALUES ($1, $2, $3, $4, TRUE)",
            &[
                &source_instance_id,
                &api_version.as_str(),
                &to_i64("credential generation", generation)?,
                &credential_id,
            ],
        )
        .map_err(backend)?;
    Ok(())
}

fn deactivate_credentials(
    transaction: &mut Transaction<'_>,
    source_instance_id: &str,
) -> StorageResult<()> {
    transaction
        .execute(
            "UPDATE cutover_credentials SET active = FALSE
             WHERE source_instance_id = $1 AND active",
            &[&source_instance_id],
        )
        .map_err(backend)?;
    Ok(())
}

fn ensure_metrics(
    transaction: &mut Transaction<'_>,
    source_instance_id: &str,
) -> StorageResult<()> {
    transaction
        .execute(
            "INSERT INTO cutover_unit_metrics (source_instance_id)
             VALUES ($1) ON CONFLICT DO NOTHING",
            &[&source_instance_id],
        )
        .map_err(backend)?;
    Ok(())
}

fn record_stale_v1(
    transaction: &mut Transaction<'_>,
    source_instance_id: &str,
) -> StorageResult<()> {
    if optional_state(transaction, source_instance_id, false)?.is_none() {
        return Ok(());
    }
    ensure_metrics(transaction, source_instance_id)?;
    transaction
        .execute(
            "UPDATE cutover_unit_metrics
             SET stale_v1_rejections = stale_v1_rejections + 1,
                 updated_at = clock_timestamp()
             WHERE source_instance_id = $1",
            &[&source_instance_id],
        )
        .map_err(backend)?;
    Ok(())
}

fn insert_metadata_value(
    meta: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    values: &mut Vec<String>,
) {
    if let Some(value) = meta
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        && !values.iter().any(|existing| existing == value)
    {
        values.push(value.to_owned());
    }
}

fn validate_transition_text(authority: &str, reason: &str) -> StorageResult<()> {
    non_blank("cutover authority", authority)?;
    non_blank("cutover reason", reason)
}

fn validate_unit(source_instance_id: &str) -> StorageResult<()> {
    non_blank("source_instance_id", source_instance_id)
}

fn validate_canonical_json(value: &str) -> StorageResult<()> {
    non_blank("canonical JSON", value)?;
    serde_json::from_str::<serde_json::Value>(value)
        .map_err(|error| StorageError::Invariant(format!("canonical JSON is invalid: {error}")))?;
    Ok(())
}

fn non_blank(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!(
            "{field} must not be blank"
        )))
    } else {
        Ok(())
    }
}

fn positive(field: &str, value: usize) -> StorageResult<()> {
    if value == 0 {
        Err(StorageError::Invariant(format!(
            "{field} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

fn usize_to_i64(field: &str, value: usize) -> StorageResult<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn to_i64(field: &str, value: u64) -> StorageResult<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn optional_i64(field: &str, value: Option<u64>) -> StorageResult<Option<i64>> {
    value.map(|value| to_i64(field, value)).transpose()
}

fn from_i64(field: &str, value: i64) -> StorageResult<u64> {
    u64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} must not be negative")))
}

fn optional_u64(field: &str, value: Option<i64>) -> StorageResult<Option<u64>> {
    value.map(|value| from_i64(field, value)).transpose()
}

fn backend(error: postgres::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}
