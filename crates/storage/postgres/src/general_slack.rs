use lethe_core::domain::{Observation, ProjectionRef};
use lethe_storage_api::{
    AppendOutcome, AuditEventRecord, DiscoveredSlackThread, ProjectionLeafWatermark,
    ProjectionWatermarkStore, SlackThreadCatalogEntry, SlackThreadCatalogStore, SlackThreadKey,
    StorageError, StorageResult,
};
use postgres::Transaction;

use super::PostgresPersistence;
use super::general::{
    append_observations_transaction, insert_audit_events, lock_partition_tree,
    observation_blob_refs, validate_append_inputs,
};
use super::general_s3::lock_blob_admission;

const KEYSPEC_VERSION: &str = "default";

impl SlackThreadCatalogStore for PostgresPersistence {
    fn append_slack_observation(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
    ) -> StorageResult<AppendOutcome> {
        self.append_slack_observation_with_audit(observation, thread, &[])
    }

    fn append_slack_observation_with_audit(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<AppendOutcome> {
        validate_thread_key(thread)?;
        validate_append_inputs(std::slice::from_ref(observation), audit_events)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&observation_blob_refs(std::slice::from_ref(
            observation,
        )))?;
        lock_partition_tree(&mut transaction)?;
        let outcome =
            append_observations_transaction(&mut transaction, std::slice::from_ref(observation))?
                .pop()
                .ok_or_else(|| {
                    StorageError::Invariant(
                        "Slack append returned no observation outcome".to_owned(),
                    )
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

    fn slack_thread_discovery_high_water(&self) -> StorageResult<u64> {
        let mut reader = self.reader()?;
        from_i64(
            "Slack thread discovery high-water",
            reader
                .query_one(
                    "SELECT discovery_high_water
                     FROM slack_thread_catalog_state WHERE singleton",
                    &[],
                )
                .map_err(backend)?
                .get(0),
        )
    }

    fn commit_slack_thread_discovery(
        &self,
        high_water: u64,
        threads: &[DiscoveredSlackThread],
    ) -> StorageResult<()> {
        let high_water = to_i64("Slack thread discovery high-water", high_water)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        let current: i64 = transaction
            .query_one(
                "SELECT discovery_high_water
                 FROM slack_thread_catalog_state
                 WHERE singleton FOR UPDATE",
                &[],
            )
            .map_err(backend)?
            .get(0);
        if high_water < current {
            return Err(StorageError::Invariant(format!(
                "Slack thread discovery high-water cannot regress from {current} to {high_water}"
            )));
        }
        let tail: i64 = transaction
            .query_one("SELECT COALESCE(MAX(append_seq), 0) FROM observations", &[])
            .map_err(backend)?
            .get(0);
        if high_water > tail {
            return Err(StorageError::Invariant(format!(
                "Slack thread discovery high-water {high_water} exceeds observation tail {tail}"
            )));
        }
        for discovered in threads {
            validate_thread_key(&discovered.key)?;
            let append_seq = to_i64(
                "Slack thread discovery append sequence",
                discovered.observation_append_seq,
            )?;
            if append_seq <= current || append_seq > high_water {
                return Err(StorageError::Invariant(format!(
                    "Slack thread discovery sequence {append_seq} is outside ({current}, {high_water}]"
                )));
            }
            let exists: bool = transaction
                .query_one(
                    "SELECT EXISTS (
                        SELECT 1 FROM observations WHERE append_seq = $1
                     )",
                    &[&append_seq],
                )
                .map_err(backend)?
                .get(0);
            if !exists {
                return Err(StorageError::Invariant(format!(
                    "Slack thread discovery sequence {append_seq} has no observation"
                )));
            }
            upsert_thread(&mut transaction, &discovered.key, append_seq)?;
        }
        transaction
            .execute(
                "UPDATE slack_thread_catalog_state
                 SET discovery_high_water = $1 WHERE singleton",
                &[&high_water],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)
    }

    fn advance_slack_thread_poll_generation(&self) -> StorageResult<u64> {
        let mut writer = self.writer()?;
        let row = writer
            .query_opt(
                "UPDATE slack_thread_catalog_state
                 SET poll_generation = poll_generation + 1
                 WHERE singleton AND poll_generation < 9223372036854775807
                 RETURNING poll_generation",
                &[],
            )
            .map_err(backend)?
            .ok_or_else(|| {
                StorageError::Invariant("Slack thread poll generation overflowed BIGINT".to_owned())
            })?;
        from_i64("Slack thread poll generation", row.get(0))
    }

    fn slack_threads_to_poll(
        &self,
        source_instance: &str,
        channel_id: &str,
        generation: u64,
        limit: usize,
    ) -> StorageResult<Vec<SlackThreadCatalogEntry>> {
        non_blank("Slack thread source_instance", source_instance)?;
        non_blank("Slack thread channel_id", channel_id)?;
        positive("Slack thread poll limit", limit)?;
        let generation = to_i64("Slack thread poll generation", generation)?;
        let limit = usize_to_i64("Slack thread poll limit", limit)?;
        let mut reader = self.reader()?;
        require_current_generation(&mut *reader, generation, "requested")?;
        thread_rows(
            reader
                .query(
                    "SELECT thread_ts, reply_cursor, active,
                            next_poll_generation, discovered_append_seq
                     FROM slack_thread_catalog
                     WHERE source_instance_id = $1
                       AND channel_id = $2
                       AND (
                            active
                            OR (NOT active AND next_poll_generation <= $3)
                       )
                     ORDER BY active DESC, next_poll_generation, thread_ts
                     LIMIT $4",
                    &[&source_instance, &channel_id, &generation, &limit],
                )
                .map_err(backend)?,
            source_instance,
            channel_id,
        )
    }

    fn complete_slack_thread_poll(
        &self,
        key: &SlackThreadKey,
        generation: u64,
        reply_cursor: &str,
        active: bool,
        next_poll_generation: u64,
    ) -> StorageResult<()> {
        validate_thread_key(key)?;
        non_blank("Slack thread reply_cursor", reply_cursor)?;
        let generation = to_i64("Slack thread poll generation", generation)?;
        let next = to_i64("Slack thread next poll generation", next_poll_generation)?;
        if next <= generation {
            return Err(StorageError::Invariant(format!(
                "Slack thread next poll generation {next} must be after completed generation {generation}"
            )));
        }
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        require_current_generation(&mut transaction, generation, "completed")?;
        let changed = transaction
            .execute(
                "UPDATE slack_thread_catalog
                 SET reply_cursor = $1, active = $2, next_poll_generation = $3
                 WHERE source_instance_id = $4
                   AND channel_id = $5
                   AND thread_ts = $6",
                &[
                    &reply_cursor,
                    &active,
                    &next,
                    &key.source_instance,
                    &key.channel_id,
                    &key.thread_ts,
                ],
            )
            .map_err(backend)?;
        if changed != 1 {
            return Err(StorageError::Invariant(format!(
                "Slack thread catalog entry not found for {}:{}:{}",
                key.source_instance, key.channel_id, key.thread_ts
            )));
        }
        transaction.commit().map_err(backend)
    }

    fn slack_thread_catalog(
        &self,
        source_instance: &str,
        channel_id: &str,
    ) -> StorageResult<Vec<SlackThreadCatalogEntry>> {
        non_blank("Slack thread source_instance", source_instance)?;
        non_blank("Slack thread channel_id", channel_id)?;
        let mut reader = self.reader()?;
        thread_rows(
            reader
                .query(
                    "SELECT thread_ts, reply_cursor, active,
                            next_poll_generation, discovered_append_seq
                     FROM slack_thread_catalog
                     WHERE source_instance_id = $1 AND channel_id = $2
                     ORDER BY thread_ts",
                    &[&source_instance, &channel_id],
                )
                .map_err(backend)?,
            source_instance,
            channel_id,
        )
    }
}

impl ProjectionWatermarkStore for PostgresPersistence {
    fn projection_leaf_watermark(
        &self,
        projection: &ProjectionRef,
        leaf_id: &str,
    ) -> StorageResult<ProjectionLeafWatermark> {
        validate_projection(projection)?;
        non_blank("projection leaf_id", leaf_id)?;
        let mut reader = self.reader()?;
        require_leaf(&mut *reader, leaf_id)?;
        let existing = reader
            .query_opt(
                "SELECT append_seq, status
                 FROM projection_leaf_watermarks
                 WHERE projection_id = $1 AND keyspec_version = $2
                   AND leaf_id = $3",
                &[&projection.as_str(), &KEYSPEC_VERSION, &leaf_id],
            )
            .map_err(backend)?;
        let (append_seq, status) = existing.map_or((0, "success".to_owned()), |row| {
            (row.get::<_, i64>(0), row.get(1))
        });
        Ok(ProjectionLeafWatermark {
            projection_id: projection.clone(),
            leaf_id: leaf_id.to_owned(),
            append_seq: from_i64("projection leaf watermark", append_seq)?,
            status,
        })
    }

    fn commit_projection_leaf_watermark(
        &self,
        watermark: &ProjectionLeafWatermark,
    ) -> StorageResult<()> {
        validate_projection(&watermark.projection_id)?;
        non_blank("projection leaf_id", &watermark.leaf_id)?;
        non_blank("projection watermark status", &watermark.status)?;
        let append_seq = to_i64("projection leaf watermark", watermark.append_seq)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        require_leaf(&mut transaction, &watermark.leaf_id)?;
        let current = transaction
            .query_opt(
                "SELECT append_seq FROM projection_leaf_watermarks
                 WHERE projection_id = $1 AND keyspec_version = $2
                   AND leaf_id = $3 FOR UPDATE",
                &[
                    &watermark.projection_id.as_str(),
                    &KEYSPEC_VERSION,
                    &watermark.leaf_id,
                ],
            )
            .map_err(backend)?
            .map_or(0, |row| row.get::<_, i64>(0));
        if append_seq < current {
            return Err(StorageError::Invariant(format!(
                "projection leaf watermark cannot decrease: {current} -> {append_seq}"
            )));
        }
        let leaf_tail: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(append_seq), 0)
                 FROM observations WHERE leaf_id = $1",
                &[&watermark.leaf_id],
            )
            .map_err(backend)?
            .get(0);
        if append_seq > leaf_tail {
            return Err(StorageError::Invariant(format!(
                "projection leaf watermark {append_seq} exceeds leaf tail {leaf_tail}"
            )));
        }
        transaction
            .execute(
                "INSERT INTO projection_leaf_watermarks (
                    projection_id, keyspec_version, leaf_id,
                    append_seq, status
                 ) VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (projection_id, keyspec_version, leaf_id)
                 DO UPDATE SET
                    append_seq = EXCLUDED.append_seq,
                    status = EXCLUDED.status,
                    updated_at = clock_timestamp()",
                &[
                    &watermark.projection_id.as_str(),
                    &KEYSPEC_VERSION,
                    &watermark.leaf_id,
                    &append_seq,
                    &watermark.status,
                ],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)
    }
}

pub(super) fn upsert_thread(
    transaction: &mut Transaction<'_>,
    key: &SlackThreadKey,
    observation_append_seq: i64,
) -> StorageResult<()> {
    validate_thread_key(key)?;
    if observation_append_seq <= 0 {
        return Err(StorageError::Invariant(
            "Slack thread discovery append sequence must be positive".to_owned(),
        ));
    }
    let generation: i64 = transaction
        .query_one(
            "SELECT poll_generation
             FROM slack_thread_catalog_state WHERE singleton",
            &[],
        )
        .map_err(backend)?
        .get(0);
    transaction
        .execute(
            "INSERT INTO slack_thread_catalog (
                source_instance_id, channel_id, thread_ts,
                discovered_append_seq, reply_cursor, active,
                next_poll_generation
             ) VALUES ($1, $2, $3, $4, $3, TRUE, $5)
             ON CONFLICT (source_instance_id, channel_id, thread_ts)
             DO UPDATE SET
                active = TRUE,
                next_poll_generation = LEAST(
                    slack_thread_catalog.next_poll_generation,
                    EXCLUDED.next_poll_generation
                ),
                discovered_append_seq = LEAST(
                    slack_thread_catalog.discovered_append_seq,
                    EXCLUDED.discovered_append_seq
                )",
            &[
                &key.source_instance,
                &key.channel_id,
                &key.thread_ts,
                &observation_append_seq,
                &generation,
            ],
        )
        .map_err(backend)?;
    Ok(())
}

fn require_current_generation(
    client: &mut impl postgres::GenericClient,
    generation: i64,
    action: &str,
) -> StorageResult<()> {
    let current: i64 = client
        .query_one(
            "SELECT poll_generation
             FROM slack_thread_catalog_state WHERE singleton",
            &[],
        )
        .map_err(backend)?
        .get(0);
    if generation != current {
        return Err(StorageError::Invariant(format!(
            "Slack thread queue {action} generation {generation}, current generation is {current}"
        )));
    }
    Ok(())
}

fn thread_rows(
    rows: Vec<postgres::Row>,
    source_instance: &str,
    channel_id: &str,
) -> StorageResult<Vec<SlackThreadCatalogEntry>> {
    rows.into_iter()
        .map(|row| {
            Ok(SlackThreadCatalogEntry {
                key: SlackThreadKey {
                    source_instance: source_instance.to_owned(),
                    channel_id: channel_id.to_owned(),
                    thread_ts: row.get(0),
                },
                reply_cursor: row.get(1),
                active: row.get(2),
                next_poll_generation: from_i64("Slack thread next poll generation", row.get(3))?,
                discovered_append_seq: from_i64(
                    "Slack thread discovered append sequence",
                    row.get(4),
                )?,
            })
        })
        .collect()
}

fn require_leaf(client: &mut impl postgres::GenericClient, leaf_id: &str) -> StorageResult<()> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM observation_leaves WHERE leaf_id = $1
             )",
            &[&leaf_id],
        )
        .map_err(backend)?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(StorageError::Invariant(format!(
            "projection leaf watermark references unknown leaf {leaf_id}"
        )))
    }
}

pub(super) fn validate_thread_key(key: &SlackThreadKey) -> StorageResult<()> {
    non_blank("Slack thread source_instance", &key.source_instance)?;
    non_blank("Slack thread channel_id", &key.channel_id)?;
    non_blank("Slack thread thread_ts", &key.thread_ts)
}

fn validate_projection(projection: &ProjectionRef) -> StorageResult<()> {
    non_blank("projection id", projection.as_str())
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

fn to_i64(field: &str, value: u64) -> StorageResult<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn usize_to_i64(field: &str, value: usize) -> StorageResult<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn from_i64(field: &str, value: i64) -> StorageResult<u64> {
    u64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} must not be negative")))
}

fn backend(error: postgres::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}
