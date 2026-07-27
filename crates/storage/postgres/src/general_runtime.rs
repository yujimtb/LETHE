use chrono::{DateTime, SecondsFormat, Utc};
use lethe_core::domain::BlobRef;
use lethe_storage_api::{
    AuditEventCursor, AuditEventRecord, PersistedSyncState, RuntimeStateStore, StorageError,
    StorageResult, SyncMetricRecord,
};

use super::PostgresPersistence;
use super::general_s3::{
    S3BlobStore, S3Object, lock_blob_admission_session, referenced_blob_refs_with_client,
    unlock_blob_admission_session,
};

impl RuntimeStateStore for PostgresPersistence {
    fn get_state(&self, key: &str) -> StorageResult<Option<String>> {
        non_blank("runtime state key", key)?;
        let mut reader = self.reader()?;
        Ok(reader
            .query_opt(
                "SELECT state_value FROM runtime_state WHERE state_key = $1",
                &[&key],
            )
            .map_err(backend)?
            .map(|row| row.get(0)))
    }

    fn set_state(&self, key: &str, value: &str) -> StorageResult<()> {
        non_blank("runtime state key", key)?;
        let mut writer = self.writer()?;
        writer
            .execute(
                "INSERT INTO runtime_state (state_key, state_value)
                 VALUES ($1, $2)
                 ON CONFLICT (state_key) DO UPDATE SET
                    state_value = EXCLUDED.state_value",
                &[&key, &value],
            )
            .map_err(backend)?;
        Ok(())
    }

    fn record_dead_letter(&self, source: &str, reason: &str) -> StorageResult<()> {
        non_blank("dead-letter source", source)?;
        non_blank("dead-letter reason", reason)?;
        let mut writer = self.writer()?;
        writer
            .execute(
                "INSERT INTO dead_letters (source, reason) VALUES ($1, $2)",
                &[&source, &reason],
            )
            .map_err(backend)?;
        Ok(())
    }

    fn record_audit_event(
        &self,
        id: &str,
        timestamp: &str,
        actor: &str,
        event_json: &str,
    ) -> StorageResult<()> {
        let event = validate_audit_event(id, timestamp, actor, event_json)?;
        let mut writer = self.writer()?;
        insert_audit_record(&mut *writer, &event)
    }

    fn audit_event_page(
        &self,
        after: Option<&AuditEventCursor>,
        limit: usize,
    ) -> StorageResult<Vec<AuditEventRecord>> {
        positive("audit event page limit", limit)?;
        let limit = usize_to_i64("audit event page limit", limit)?;
        let (after_timestamp, after_id) = after
            .map(|cursor| {
                Ok((
                    Some(canonical_timestamp(
                        "audit event cursor timestamp",
                        &cursor.timestamp,
                    )?),
                    Some(non_blank_owned("audit event cursor id", &cursor.id)?),
                ))
            })
            .transpose()?
            .unwrap_or((None, None));
        let mut reader = self.reader()?;
        reader
            .query(
                "SELECT audit_id, timestamp_text, actor, event_json
                 FROM audit_events
                 WHERE $1::text IS NULL
                    OR timestamp_text > $1
                    OR (timestamp_text = $1 AND audit_id > $2)
                 ORDER BY timestamp_text, audit_id
                 LIMIT $3",
                &[&after_timestamp, &after_id, &limit],
            )
            .map_err(backend)?
            .into_iter()
            .map(|row| {
                let event = AuditEventRecord {
                    id: row.get(0),
                    timestamp: row.get(1),
                    actor: row.get(2),
                    event_json: row.get(3),
                };
                validate_stored_audit(event)
            })
            .collect()
    }

    fn record_sync_metrics(&self, source: &str, metrics: &SyncMetricRecord) -> StorageResult<()> {
        self.record_sync_state(
            source,
            &PersistedSyncState {
                metrics: metrics.clone(),
                completed_at: Utc::now(),
                error: None,
            },
        )
    }

    fn record_sync_state(&self, source: &str, state: &PersistedSyncState) -> StorageResult<()> {
        non_blank("sync source", source)?;
        let json = sync_state_json(state)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        transaction
            .execute(
                "INSERT INTO sync_state (source, state_json)
                 VALUES ($1, $2::text::jsonb)
                 ON CONFLICT (source) DO UPDATE SET
                    state_json = EXCLUDED.state_json",
                &[&source, &json],
            )
            .map_err(backend)?;
        transaction
            .execute(
                "INSERT INTO sync_metrics (source, recorded_at, metrics_json)
                 VALUES ($1, $2::text::timestamptz, $3::text::jsonb)",
                &[
                    &source,
                    &canonical_utc(state.completed_at),
                    &metrics_json(&state.metrics)?.to_string(),
                ],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)
    }

    fn load_sync_state(&self, source: &str) -> StorageResult<Option<PersistedSyncState>> {
        non_blank("sync source", source)?;
        let mut reader = self.reader()?;
        reader
            .query_opt(
                "SELECT state_json::text FROM sync_state WHERE source = $1",
                &[&source],
            )
            .map_err(backend)?
            .map(|row| parse_sync_state(row.get(0)))
            .transpose()
    }

    fn apply_retention(&self, retention_days: u32) -> StorageResult<usize> {
        if retention_days == 0 {
            return Err(StorageError::Invariant(
                "retention_days must be greater than zero".to_owned(),
            ));
        }
        let days = i32::try_from(retention_days).map_err(|_| {
            StorageError::Invariant("retention_days exceeds PostgreSQL INTEGER".to_owned())
        })?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        let dead_letters = transaction
            .execute(
                "DELETE FROM dead_letters
                 WHERE created_at < clock_timestamp() - make_interval(days => $1)",
                &[&days],
            )
            .map_err(backend)?;
        let audits = transaction
            .execute(
                "DELETE FROM audit_events
                 WHERE timestamp_text::timestamptz
                     < clock_timestamp() - make_interval(days => $1)",
                &[&days],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)?;
        usize::try_from(dead_letters + audits)
            .map_err(|_| StorageError::Invariant("retention count exceeds usize".to_owned()))
    }

    fn garbage_collect_orphan_blobs(&self) -> StorageResult<usize> {
        garbage_collect_orphan_blobs(self)
    }

    fn deep_check(&self) -> StorageResult<()> {
        deep_check_database(self)?;
        self.admitted_blob_store()?.deep_probe()?;
        let references = self.referenced_blob_refs()?;
        self.verify_blob_references_admitted(
            &references.into_iter().map(BlobRef::new).collect::<Vec<_>>(),
        )?;
        Ok(())
    }
}

impl PostgresPersistence {
    /// Run only the PostgreSQL portion of the deep check for database-only
    /// conformance fixtures.
    ///
    /// Production readiness must call `RuntimeStateStore::deep_check`, which
    /// additionally requires an admitted S3 store and executes a PUT/GET/DELETE
    /// probe.
    pub fn deep_check_database_only_for_tests(&self) -> StorageResult<()> {
        deep_check_database(self)
    }
}

fn deep_check_database(store: &PostgresPersistence) -> StorageResult<()> {
    store.deep_check_connections()?;
    let mut reader = store.reader()?;
    let violations: Vec<String> = reader
        .query(
            "SELECT violation FROM (
                SELECT 'general_storage_pin_count' AS violation
                WHERE (SELECT COUNT(*) FROM general_storage_pin) <> 1
                UNION ALL
                SELECT 'root_leaf_count'
                WHERE (
                    SELECT COUNT(*) FROM observation_leaves
                    WHERE parent_leaf_id IS NULL
                ) <> 1
                UNION ALL
                SELECT 'observation_on_inactive_leaf'
                WHERE EXISTS (
                    SELECT 1 FROM observations
                    JOIN observation_leaves USING (leaf_id)
                    WHERE NOT observation_leaves.active
                )
                UNION ALL
                SELECT 'leaf_observation_count'
                WHERE EXISTS (
                    SELECT 1 FROM observation_leaves leaves
                    WHERE leaves.observation_count <> (
                        SELECT COUNT(*) FROM observations
                        WHERE observations.leaf_id = leaves.leaf_id
                    )
                )
                UNION ALL
                SELECT 'active_generation_is_retired'
                WHERE EXISTS (
                    SELECT 1
                    FROM projection_materialization_heads heads
                    JOIN retired_projection_materializations retired
                      USING (projection_id, keyspec_version, generation)
                )
                UNION ALL
                SELECT 'slack_catalog_high_water'
                WHERE EXISTS (
                    SELECT 1 FROM slack_thread_catalog
                    WHERE discovered_append_seq > (
                        SELECT discovery_high_water
                        FROM slack_thread_catalog_state WHERE singleton
                    )
                )
                UNION ALL
                SELECT 'projection_watermark_beyond_lake'
                WHERE EXISTS (
                    SELECT 1 FROM projection_leaf_watermarks
                    WHERE append_seq > (
                        SELECT COALESCE(MAX(append_seq), 0) FROM observations
                    )
                )
             ) checks",
            &[],
        )
        .map_err(backend)?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if !violations.is_empty() {
        return Err(StorageError::Invariant(format!(
            "PostgreSQL deep check failed: {}",
            violations.join(", ")
        )));
    }
    for row in reader
        .query("SELECT observation_json::text FROM observations", &[])
        .map_err(backend)?
    {
        let json: String = row.get(0);
        serde_json::from_str::<lethe_core::domain::Observation>(&json).map_err(|error| {
            StorageError::Invariant(format!(
                "stored observation JSON violates the domain schema: {error}"
            ))
        })?;
    }
    for row in reader
        .query("SELECT supplemental_json::text FROM supplementals", &[])
        .map_err(backend)?
    {
        let json: String = row.get(0);
        serde_json::from_str::<lethe_core::domain::SupplementalRecord>(&json).map_err(|error| {
            StorageError::Invariant(format!(
                "stored supplemental JSON violates the domain schema: {error}"
            ))
        })?;
    }
    for row in reader
        .query("SELECT state_json::text FROM sync_state", &[])
        .map_err(backend)?
    {
        parse_sync_state(row.get(0))?;
    }
    Ok(())
}

fn garbage_collect_orphan_blobs(store: &PostgresPersistence) -> StorageResult<usize> {
    let blob_store = store.admitted_blob_store()?;
    let objects = blob_store.list_objects()?;
    let references = store.referenced_blob_refs()?;
    let now = Utc::now();
    let object_refs = objects
        .iter()
        .map(|object| {
            blob_ref_for_object(object).map(|blob_ref| (blob_ref.as_str().to_owned(), object))
        })
        .collect::<StorageResult<std::collections::BTreeMap<_, _>>>()?;

    let mut writer = store.writer()?;
    let mut transaction = writer.transaction().map_err(backend)?;
    let generation: i64 = transaction
        .query_one(
            "UPDATE blob_orphan_scan_state
             SET scan_generation = scan_generation + 1
             WHERE singleton AND scan_generation < 9223372036854775807
             RETURNING scan_generation",
            &[],
        )
        .map_err(backend)?
        .get(0);
    let existing_candidates = transaction
        .query("SELECT blob_ref FROM blob_orphan_candidates", &[])
        .map_err(backend)?
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    for candidate in existing_candidates {
        if references.contains(&candidate) || !object_refs.contains_key(&candidate) {
            transaction
                .execute(
                    "DELETE FROM blob_orphan_candidates WHERE blob_ref = $1",
                    &[&candidate],
                )
                .map_err(backend)?;
        }
    }
    for (blob_ref, object) in &object_refs {
        if references.contains(blob_ref) || !object_old_enough(blob_store, object, now)? {
            continue;
        }
        let previous = transaction
            .query_opt(
                "SELECT last_scan_generation, consecutive_scans
                 FROM blob_orphan_candidates WHERE blob_ref = $1
                 FOR UPDATE",
                &[&blob_ref],
            )
            .map_err(backend)?;
        let consecutive = previous.map_or(1_i32, |row| {
            let last_generation: i64 = row.get(0);
            let count: i32 = row.get(1);
            if last_generation == generation - 1 {
                count.saturating_add(1)
            } else {
                1
            }
        });
        let first_unreferenced_at = canonical_utc(now);
        let byte_count = i64::try_from(object.byte_count).map_err(|_| {
            StorageError::Invariant("orphan object byte count exceeds BIGINT".to_owned())
        })?;
        transaction
            .execute(
                "INSERT INTO blob_orphan_candidates (
                    blob_ref, object_key, byte_count, first_unreferenced_at,
                    last_scan_generation, consecutive_scans
                 ) VALUES ($1, $2, $3, $4::text::timestamptz, $5, $6)
                 ON CONFLICT (blob_ref) DO UPDATE SET
                    object_key = EXCLUDED.object_key,
                    byte_count = EXCLUDED.byte_count,
                    first_unreferenced_at = CASE
                        WHEN blob_orphan_candidates.last_scan_generation = $5 - 1
                        THEN blob_orphan_candidates.first_unreferenced_at
                        ELSE EXCLUDED.first_unreferenced_at
                    END,
                    last_scan_generation = EXCLUDED.last_scan_generation,
                    consecutive_scans = EXCLUDED.consecutive_scans",
                &[
                    &blob_ref,
                    &object.object_key,
                    &byte_count,
                    &first_unreferenced_at,
                    &generation,
                    &consecutive,
                ],
            )
            .map_err(backend)?;
    }
    let eligible = transaction
        .query(
            "SELECT blob_ref FROM blob_orphan_candidates
             WHERE last_scan_generation = $1 AND consecutive_scans >= 2
             ORDER BY blob_ref",
            &[&generation],
        )
        .map_err(backend)?
        .into_iter()
        .map(|row| BlobRef::new(row.get::<_, String>(0)))
        .collect::<Vec<_>>();
    transaction.commit().map_err(backend)?;
    drop(writer);

    let mut deleted = 0_usize;
    for blob_ref in eligible {
        if delete_eligible_orphan(store, blob_store, generation, &blob_ref)? {
            deleted = deleted.checked_add(1).ok_or_else(|| {
                StorageError::Invariant("orphan deletion count overflow".to_owned())
            })?;
        }
    }
    Ok(deleted)
}

fn delete_eligible_orphan(
    store: &PostgresPersistence,
    blob_store: &S3BlobStore,
    generation: i64,
    blob_ref: &BlobRef,
) -> StorageResult<bool> {
    let mut writer = store.writer()?;
    lock_blob_admission_session(&mut writer)?;
    let result = delete_eligible_orphan_locked(&mut writer, blob_store, generation, blob_ref);
    let unlock = unlock_blob_admission_session(&mut writer);
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
    }
}

fn delete_eligible_orphan_locked(
    writer: &mut postgres::Client,
    blob_store: &S3BlobStore,
    generation: i64,
    blob_ref: &BlobRef,
) -> StorageResult<bool> {
    let mut transaction = writer.transaction().map_err(backend)?;
    let references = referenced_blob_refs_with_client(&mut transaction)?;
    if references.contains(blob_ref.as_str()) {
        transaction
            .execute(
                "DELETE FROM blob_orphan_candidates WHERE blob_ref = $1",
                &[&blob_ref.as_str()],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)?;
        return Ok(false);
    }
    let still_eligible: bool = transaction
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM blob_orphan_candidates
                WHERE blob_ref = $1
                  AND last_scan_generation = $2
                  AND consecutive_scans >= 2
             )",
            &[&blob_ref.as_str(), &generation],
        )
        .map_err(backend)?
        .get(0);
    if !still_eligible {
        transaction.commit().map_err(backend)?;
        return Ok(false);
    }
    let audit = AuditEventRecord {
        id: format!(
            "audit:blob-orphan-delete:{generation}:{}",
            blob_ref.as_str()
        ),
        timestamp: canonical_utc(Utc::now()),
        actor: "actor:postgres-s3-gc".to_owned(),
        event_json: serde_json::json!({
            "event": "blob_orphan_delete",
            "blob_ref": blob_ref.as_str(),
            "scan_generation": generation,
        })
        .to_string(),
    };
    super::general_runtime::insert_audit_record(&mut transaction, &audit)?;
    transaction
        .execute(
            "DELETE FROM blob_objects WHERE blob_ref = $1",
            &[&blob_ref.as_str()],
        )
        .map_err(backend)?;
    transaction.commit().map_err(backend)?;

    blob_store.delete_object(blob_ref)?;
    writer
        .execute(
            "DELETE FROM blob_orphan_candidates WHERE blob_ref = $1",
            &[&blob_ref.as_str()],
        )
        .map_err(backend)?;
    Ok(true)
}

fn blob_ref_for_object(object: &S3Object) -> StorageResult<BlobRef> {
    let digest = object.object_key.strip_prefix("sha256/").ok_or_else(|| {
        StorageError::Invariant(format!(
            "S3 object key has unexpected prefix: {:?}",
            object.object_key
        ))
    })?;
    let blob_ref = BlobRef::new(format!("blob:sha256:{digest}"));
    let _ = lethe_storage_api::blob_ref_sha256(&blob_ref)?;
    Ok(blob_ref)
}

fn object_old_enough(
    store: &S3BlobStore,
    object: &S3Object,
    now: DateTime<Utc>,
) -> StorageResult<bool> {
    let age = now.signed_duration_since(object.last_modified);
    if age < chrono::Duration::zero() {
        return Ok(false);
    }
    let age = age.to_std().map_err(|error| {
        StorageError::Invariant(format!("S3 object age cannot convert to duration: {error}"))
    })?;
    Ok(age >= store.orphan_min_age())
}

pub(super) fn insert_audit_record(
    client: &mut impl postgres::GenericClient,
    event: &AuditEventRecord,
) -> StorageResult<()> {
    let event = validate_audit_record(event)?;
    client
        .execute(
            "INSERT INTO audit_events (
                audit_id, timestamp_text, actor, event_json
             ) VALUES ($1, $2, $3, $4)",
            &[&event.id, &event.timestamp, &event.actor, &event.event_json],
        )
        .map_err(backend)?;
    Ok(())
}

pub(super) fn validate_audit_record(event: &AuditEventRecord) -> StorageResult<AuditEventRecord> {
    validate_audit_event(&event.id, &event.timestamp, &event.actor, &event.event_json)
}

fn validate_audit_event(
    id: &str,
    timestamp: &str,
    actor: &str,
    event_json: &str,
) -> StorageResult<AuditEventRecord> {
    let event = AuditEventRecord {
        id: non_blank_owned("audit id", id)?,
        timestamp: canonical_timestamp("audit timestamp", timestamp)?,
        actor: non_blank_owned("audit actor", actor)?,
        event_json: event_json.to_owned(),
    };
    validate_stored_audit(event)
}

fn validate_stored_audit(event: AuditEventRecord) -> StorageResult<AuditEventRecord> {
    non_blank("audit id", &event.id)?;
    non_blank("audit actor", &event.actor)?;
    let canonical = canonical_timestamp("audit timestamp", &event.timestamp)?;
    if event.timestamp != canonical {
        return Err(StorageError::Invariant(format!(
            "stored audit timestamp is not canonical UTC: {:?}",
            event.timestamp
        )));
    }
    serde_json::from_str::<serde_json::Value>(&event.event_json).map_err(|error| {
        StorageError::Invariant(format!("audit event_json is invalid JSON: {error}"))
    })?;
    Ok(event)
}

fn sync_state_json(state: &PersistedSyncState) -> StorageResult<String> {
    Ok(serde_json::json!({
        "metrics": metrics_json(&state.metrics)?,
        "completed_at": canonical_utc(state.completed_at),
        "error": state.error,
    })
    .to_string())
}

fn metrics_json(metrics: &SyncMetricRecord) -> StorageResult<serde_json::Value> {
    Ok(serde_json::json!({
        "fetched": metrics.fetched,
        "ingested": metrics.ingested,
        "skipped": metrics.skipped,
        "failed": metrics.failed,
        "quarantined": metrics.quarantined,
        "latency_ms": metrics.latency_ms,
    }))
}

fn parse_sync_state(json: String) -> StorageResult<PersistedSyncState> {
    let value: serde_json::Value = serde_json::from_str(&json)
        .map_err(|error| StorageError::Invariant(format!("invalid sync state JSON: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| StorageError::Invariant("sync state must be an object".to_owned()))?;
    let metrics = object_field(object, "metrics")?
        .as_object()
        .ok_or_else(|| {
            StorageError::Invariant("sync state metrics must be an object".to_owned())
        })?;
    let error = match object_field(object, "error")? {
        serde_json::Value::Null => None,
        serde_json::Value::String(value) => Some(value.clone()),
        _ => {
            return Err(StorageError::Invariant(
                "sync state error must be a string or null".to_owned(),
            ));
        }
    };
    let completed_at = object_field(object, "completed_at")?
        .as_str()
        .ok_or_else(|| {
            StorageError::Invariant("sync state completed_at must be a string".to_owned())
        })?;
    let completed_at = DateTime::parse_from_rfc3339(completed_at)
        .map_err(|error| {
            StorageError::Invariant(format!("sync state completed_at is invalid: {error}"))
        })?
        .with_timezone(&Utc);
    Ok(PersistedSyncState {
        metrics: SyncMetricRecord {
            fetched: metric(metrics, "fetched")?,
            ingested: metric(metrics, "ingested")?,
            skipped: metric(metrics, "skipped")?,
            failed: metric(metrics, "failed")?,
            quarantined: metric(metrics, "quarantined")?,
            latency_ms: metric(metrics, "latency_ms")?,
        },
        completed_at,
        error,
    })
}

fn object_field<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> StorageResult<&'a serde_json::Value> {
    object.get(name).ok_or_else(|| {
        StorageError::Invariant(format!("sync state is missing required field {name}"))
    })
}

fn metric(metrics: &serde_json::Map<String, serde_json::Value>, name: &str) -> StorageResult<u64> {
    metrics
        .get(name)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            StorageError::Invariant(format!(
                "sync state metric {name} must be an unsigned integer"
            ))
        })
}

fn canonical_timestamp(field: &str, value: &str) -> StorageResult<String> {
    non_blank(field, value)?;
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|error| StorageError::Invariant(format!("{field} is not RFC 3339: {error}")))?;
    Ok(canonical_utc(parsed.with_timezone(&Utc)))
}

fn canonical_utc(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::AutoSi, true)
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

fn non_blank_owned(field: &str, value: &str) -> StorageResult<String> {
    non_blank(field, value)?;
    Ok(value.to_owned())
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

fn backend(error: postgres::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}
