mod cutover;
mod schema;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key};
use arc_swap::ArcSwapOption;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::Digest;

use lethe_core::domain::{
    BlobRef, DataSpaceId, IdempotencyKey, Observation, ObservationId, OperationalEventId,
    SupplementalId, SupplementalRecord, observation_privacy_keys,
};
use lethe_runtime::runtime::partition::{
    PARTITION_EVENT_FAILOVER, PARTITION_EVENT_INITIALIZE, PARTITION_EVENT_RECOVER,
    PARTITION_EVENT_SPLIT_COMMIT, PARTITION_EVENT_SPLIT_PREPARE, PARTITION_SPLIT_REASON_CAPACITY,
    PartitionTree, RoutedObservation, RoutingKeyOrder, failover_event_json, identity_keyspec_json,
    initialize_event_json, parse_partition_event, plan_capacity_split, recover_event_json,
    routing_key_from_observation_for_order, routing_keyspec_json_for_order,
    split_commit_event_json, split_prepare_event_json,
};
use lethe_storage_api::{
    AppendOutcome as PortAppendOutcome, BlobStore as BlobStorePort, CutoverApiVersion,
    CutoverBlocker, CutoverFixture, CutoverHealth, CutoverInventoryItem, CutoverPhase,
    CutoverReadinessReport, CutoverState, CutoverStore, DiscoveredSlackThread,
    IdentityBridgeBatchReport, IdentityBridgeResolution, LeafPosition, ObservationStats,
    ObservationStore as ObservationStorePort, OperationalAppendOutcome, OperationalAppendRequest,
    OperationalEventFilter, OperationalEventStats, OperationalEventStore, PersistedSyncState,
    ProjectionItem, ProjectionItemCommit, ProjectionLeafWatermark,
    ProjectionMaterializer as ProjectionMaterializerPort,
    ProjectionWatermarkStore as ProjectionWatermarkStorePort, RehomeMode as PortRehomeMode,
    RuntimeStateStore as RuntimeStateStorePort, SlackThreadCatalogEntry,
    SlackThreadCatalogStore as SlackThreadCatalogStorePort, SlackThreadKey, StorageError,
    StorageResult, StoredObservation, StoredOperationalEvent,
    SupplementalProjectionCommitter as SupplementalProjectionCommitterPort,
    SupplementalStore as SupplementalStorePort, SyncMetricRecord,
};

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema invariant violation: {0}")]
    SchemaInvariant(String),
    #[error("cutover admission denied: {0}")]
    CutoverAdmissionDenied(String),
}

pub struct SqlitePersistence {
    conn: Connection,
    blob_dir: PathBuf,
    secret_encryption_key: [u8; 32],
    routing_key_order: RoutingKeyOrder,
    partition_tree: ArcSwapOption<PartitionTree>,
}

pub struct SqliteOperationalEventStore {
    persistence: SqlitePersistence,
    data_space_id: DataSpaceId,
}

const SCHEMA_VERSION_IDENTITY_LOOKUP_INDEX: i64 = 9;
const SCHEMA_VERSION_LOCK_SPLIT_SCALARS: i64 = 10;
const SCHEMA_VERSION_KEYSET_READS: i64 = 11;
const SCHEMA_VERSION_PRIVACY_PROJECTION: i64 = 12;
const SCHEMA_VERSION_RECONSENT_PRIVACY_INDEX: i64 = 13;
const SCHEMA_VERSION_CUTOVER_BRIDGE: i64 = 14;
#[cfg(test)]
const CURRENT_SCHEMA_VERSION: i64 = SCHEMA_VERSION_CUTOVER_BRIDGE;
const CANONICAL_JSON_META_KEY: &str = "canonical_json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableAppendOutcome {
    Appended(ObservationId),
    Duplicate(ObservationId),
    CanonicalCollision(ObservationId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RehomeMode {
    StoredIdentity,
    RecomputedIdentity {
        identity_key: IdempotencyKey,
        canonical_json: String,
    },
}

#[derive(Debug, Clone)]
pub struct BlueGreenTransform {
    pub identity_key: IdempotencyKey,
    pub canonical_json: String,
    pub routing_key: String,
}

impl SqlitePersistence {
    pub fn open(
        database_path: &Path,
        blob_dir: &Path,
        secret_encryption_key: &[u8; 32],
    ) -> Result<Self, PersistenceError> {
        Self::open_with_routing_key_order(
            database_path,
            blob_dir,
            secret_encryption_key,
            RoutingKeyOrder::MonthYearSourceContainerPublished,
        )
    }

    pub fn open_with_routing_key_order(
        database_path: &Path,
        blob_dir: &Path,
        secret_encryption_key: &[u8; 32],
        routing_key_order: RoutingKeyOrder,
    ) -> Result<Self, PersistenceError> {
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(blob_dir)?;

        let conn = Connection::open(database_path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
        let store = Self {
            conn,
            blob_dir: blob_dir.to_path_buf(),
            secret_encryption_key: *secret_encryption_key,
            routing_key_order,
            partition_tree: ArcSwapOption::empty(),
        };
        store.init_schema()?;
        let partition_tree = store.rebuild_partition_tree_from_log()?;
        store.partition_tree.store(Some(Arc::new(partition_tree)));
        Ok(store)
    }

    pub fn load_observations(&self) -> Result<Vec<Observation>, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare("SELECT observation_json FROM observations ORDER BY append_seq")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut observations = Vec::new();
        for row in rows {
            let json = row?;
            observations.push(serde_json::from_str::<Observation>(&json)?);
        }
        Ok(observations)
    }

    pub fn observation_stats(&self) -> Result<ObservationStats, PersistenceError> {
        self.conn
            .query_row(
                "SELECT observation_count, max_append_seq
                 FROM observation_stats
                 WHERE singleton = 1",
                [],
                |row| {
                    Ok(ObservationStats {
                        count: row.get(0)?,
                        max_append_seq: row.get(1)?,
                    })
                },
            )
            .map_err(PersistenceError::from)
    }

    pub fn load_supplementals(&self) -> Result<Vec<SupplementalRecord>, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare("SELECT supplemental_json FROM supplementals ORDER BY created_at, id")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut supplementals = Vec::new();
        for row in rows {
            let json = row?;
            supplementals.push(serde_json::from_str::<SupplementalRecord>(&json)?);
        }
        Ok(supplementals)
    }

    pub fn persist_observation(&self, observation: &Observation) -> Result<(), PersistenceError> {
        match self.append_observation_idempotent(observation)? {
            DurableAppendOutcome::Appended(_) => Ok(()),
            DurableAppendOutcome::Duplicate(existing_id) => Err(PersistenceError::SchemaInvariant(
                format!("duplicate observation already exists: {existing_id}"),
            )),
            DurableAppendOutcome::CanonicalCollision(existing_id) => {
                Err(PersistenceError::SchemaInvariant(format!(
                    "identity key collision with existing observation: {existing_id}"
                )))
            }
        }
    }

    pub fn append_observation_idempotent(
        &self,
        observation: &Observation,
    ) -> Result<DurableAppendOutcome, PersistenceError> {
        let mut outcomes =
            self.append_observations_idempotent(std::slice::from_ref(observation))?;
        Ok(outcomes.remove(0))
    }

    pub fn append_observations_idempotent(
        &self,
        observations: &[Observation],
    ) -> Result<Vec<DurableAppendOutcome>, PersistenceError> {
        self.append_observations_idempotent_with_audit(observations, &[])
    }

    pub fn append_observations_idempotent_with_audit(
        &self,
        observations: &[Observation],
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> Result<Vec<DurableAppendOutcome>, PersistenceError> {
        let tree = self.partition_tree_snapshot()?;
        let transaction = self.conn.unchecked_transaction()?;
        let outcomes = append_observations_in_transaction(
            &transaction,
            &tree,
            self.routing_key_order,
            observations,
        )?;
        for audit in audit_events {
            insert_audit_event(&transaction, audit)?;
        }

        transaction.commit()?;
        Ok(outcomes)
    }

    pub fn append_slack_observation(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
    ) -> Result<DurableAppendOutcome, PersistenceError> {
        self.append_slack_observation_with_audit(observation, thread, &[])
    }

    pub fn append_slack_observation_with_audit(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> Result<DurableAppendOutcome, PersistenceError> {
        validate_slack_thread_key(thread)?;
        let tree = self.partition_tree_snapshot()?;
        let transaction = self.conn.unchecked_transaction()?;
        let mut outcomes = append_observations_in_transaction(
            &transaction,
            &tree,
            self.routing_key_order,
            std::slice::from_ref(observation),
        )?;
        let outcome = outcomes.remove(0);
        if let DurableAppendOutcome::Appended(observation_id)
        | DurableAppendOutcome::Duplicate(observation_id) = &outcome
        {
            let append_seq = transaction.query_row(
                "SELECT append_seq FROM observations WHERE id = ?1",
                [observation_id.as_str()],
                |row| row.get::<_, u64>(0),
            )?;
            upsert_slack_thread(&transaction, thread, append_seq)?;
        }
        for audit in audit_events {
            insert_audit_event(&transaction, audit)?;
        }
        transaction.commit()?;
        Ok(outcome)
    }

    pub fn rehome_observation(
        &self,
        observation: &Observation,
        mode: RehomeMode,
    ) -> Result<DurableAppendOutcome, PersistenceError> {
        let mut rehomed = observation.clone();
        match mode {
            RehomeMode::StoredIdentity => {
                require_identity_and_canonical_json(&rehomed)?;
            }
            RehomeMode::RecomputedIdentity {
                identity_key,
                canonical_json,
            } => {
                rehomed.idempotency_key = identity_key;
                let mut meta = match rehomed.meta {
                    serde_json::Value::Object(map) => map,
                    _ => serde_json::Map::new(),
                };
                meta.insert(
                    CANONICAL_JSON_META_KEY.to_owned(),
                    serde_json::Value::String(canonical_json),
                );
                rehomed.meta = serde_json::Value::Object(meta);
            }
        }

        self.append_observation_idempotent(&rehomed)
    }

    pub fn persist_supplemental(
        &self,
        record: &SupplementalRecord,
    ) -> Result<(), PersistenceError> {
        let json = serde_json::to_string(record)?;
        self.conn.execute(
            "INSERT INTO supplementals (id, created_at, supplemental_json) VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET created_at = excluded.created_at, supplemental_json = excluded.supplemental_json",
            params![
                record.id.as_str(),
                record.created_at.to_rfc3339(),
                json,
            ],
        )?;
        Ok(())
    }

    pub fn get_state(&self, key: &str) -> Result<Option<String>, PersistenceError> {
        self.conn
            .query_row(
                "SELECT value FROM sync_state WHERE key = ?1",
                [key],
                |row| row.get(0),
            )
            .optional()
            .map_err(PersistenceError::from)
    }

    pub fn set_state(&self, key: &str, value: &str) -> Result<(), PersistenceError> {
        self.conn.execute(
            "INSERT INTO sync_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn slack_thread_discovery_high_water(&self) -> Result<u64, PersistenceError> {
        self.conn
            .query_row(
                "SELECT discovery_high_water
                 FROM slack_thread_catalog_state
                 WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(PersistenceError::from)
    }

    pub fn commit_slack_thread_discovery(
        &self,
        high_water: u64,
        threads: &[DiscoveredSlackThread],
    ) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        let current = transaction.query_row(
            "SELECT discovery_high_water
             FROM slack_thread_catalog_state
             WHERE singleton = 1",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        if high_water < current {
            return Err(PersistenceError::SchemaInvariant(format!(
                "Slack thread discovery high-water cannot regress from {current} to {high_water}"
            )));
        }
        let max_append_seq = transaction.query_row(
            "SELECT COALESCE(MAX(append_seq), 0) FROM observations",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        if high_water > max_append_seq {
            return Err(PersistenceError::SchemaInvariant(format!(
                "Slack thread discovery high-water {high_water} exceeds observation tail {max_append_seq}"
            )));
        }
        for thread in threads {
            validate_slack_thread_key(&thread.key)?;
            if thread.observation_append_seq <= current
                || thread.observation_append_seq > high_water
            {
                return Err(PersistenceError::SchemaInvariant(format!(
                    "Slack thread discovery sequence {} is outside ({current}, {high_water}]",
                    thread.observation_append_seq
                )));
            }
            upsert_slack_thread(&transaction, &thread.key, thread.observation_append_seq)?;
        }
        transaction.execute(
            "UPDATE slack_thread_catalog_state
             SET discovery_high_water = ?1
             WHERE singleton = 1",
            [high_water],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn advance_slack_thread_poll_generation(&self) -> Result<u64, PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        let current = transaction.query_row(
            "SELECT poll_generation
             FROM slack_thread_catalog_state
             WHERE singleton = 1",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        let next = current.checked_add(1).ok_or_else(|| {
            PersistenceError::SchemaInvariant(
                "Slack thread poll generation overflowed u64".to_owned(),
            )
        })?;
        transaction.execute(
            "UPDATE slack_thread_catalog_state
             SET poll_generation = ?1
             WHERE singleton = 1",
            [next],
        )?;
        transaction.commit()?;
        Ok(next)
    }

    pub fn slack_threads_to_poll(
        &self,
        source_instance: &str,
        channel_id: &str,
        generation: u64,
        limit: usize,
    ) -> Result<Vec<SlackThreadCatalogEntry>, PersistenceError> {
        validate_non_blank("source_instance", source_instance)?;
        validate_non_blank("channel_id", channel_id)?;
        if limit == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "Slack thread poll limit must be greater than zero".to_owned(),
            ));
        }
        let current = self.conn.query_row(
            "SELECT poll_generation
             FROM slack_thread_catalog_state
             WHERE singleton = 1",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        if generation != current {
            return Err(PersistenceError::SchemaInvariant(format!(
                "Slack thread queue requested generation {generation}, current generation is {current}"
            )));
        }
        let mut statement = self.conn.prepare(
            "SELECT thread_ts, reply_cursor, active, next_poll_generation,
                    discovered_append_seq
             FROM (
                SELECT thread_ts, reply_cursor, active, next_poll_generation,
                       discovered_append_seq
                FROM slack_thread_catalog
                WHERE source_instance = ?1
                  AND channel_id = ?2
                  AND active = 1
                UNION ALL
                SELECT thread_ts, reply_cursor, active, next_poll_generation,
                       discovered_append_seq
                FROM slack_thread_catalog
                WHERE source_instance = ?1
                  AND channel_id = ?2
                  AND active = 0
                  AND next_poll_generation <= ?3
             )
             ORDER BY active DESC, next_poll_generation, thread_ts
             LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![source_instance, channel_id, generation, limit],
            |row| {
                Ok(SlackThreadCatalogEntry {
                    key: SlackThreadKey {
                        source_instance: source_instance.to_owned(),
                        channel_id: channel_id.to_owned(),
                        thread_ts: row.get(0)?,
                    },
                    reply_cursor: row.get(1)?,
                    active: row.get::<_, i64>(2)? != 0,
                    next_poll_generation: row.get(3)?,
                    discovered_append_seq: row.get(4)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::from)
    }

    pub fn complete_slack_thread_poll(
        &self,
        key: &SlackThreadKey,
        generation: u64,
        reply_cursor: &str,
        active: bool,
        next_poll_generation: u64,
    ) -> Result<(), PersistenceError> {
        validate_slack_thread_key(key)?;
        validate_non_blank("reply_cursor", reply_cursor)?;
        if next_poll_generation <= generation {
            return Err(PersistenceError::SchemaInvariant(format!(
                "Slack thread next poll generation {next_poll_generation} must be after completed generation {generation}"
            )));
        }
        let transaction = self.conn.unchecked_transaction()?;
        let current = transaction.query_row(
            "SELECT poll_generation
             FROM slack_thread_catalog_state
             WHERE singleton = 1",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        if generation != current {
            return Err(PersistenceError::SchemaInvariant(format!(
                "Slack thread poll completed generation {generation}, current generation is {current}"
            )));
        }
        let changed = transaction.execute(
            "UPDATE slack_thread_catalog
             SET reply_cursor = ?1,
                 active = ?2,
                 next_poll_generation = ?3
             WHERE source_instance = ?4
               AND channel_id = ?5
               AND thread_ts = ?6",
            params![
                reply_cursor,
                i64::from(active),
                next_poll_generation,
                key.source_instance,
                key.channel_id,
                key.thread_ts,
            ],
        )?;
        if changed != 1 {
            return Err(PersistenceError::SchemaInvariant(format!(
                "Slack thread catalog entry not found for {}:{}:{}",
                key.source_instance, key.channel_id, key.thread_ts
            )));
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn slack_thread_catalog(
        &self,
        source_instance: &str,
        channel_id: &str,
    ) -> Result<Vec<SlackThreadCatalogEntry>, PersistenceError> {
        validate_non_blank("source_instance", source_instance)?;
        validate_non_blank("channel_id", channel_id)?;
        let mut statement = self.conn.prepare(
            "SELECT thread_ts, reply_cursor, active, next_poll_generation,
                    discovered_append_seq
             FROM slack_thread_catalog
             WHERE source_instance = ?1 AND channel_id = ?2
             ORDER BY thread_ts",
        )?;
        let rows = statement.query_map(params![source_instance, channel_id], |row| {
            Ok(SlackThreadCatalogEntry {
                key: SlackThreadKey {
                    source_instance: source_instance.to_owned(),
                    channel_id: channel_id.to_owned(),
                    thread_ts: row.get(0)?,
                },
                reply_cursor: row.get(1)?,
                active: row.get::<_, i64>(2)? != 0,
                next_poll_generation: row.get(3)?,
                discovered_append_seq: row.get(4)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::from)
    }

    pub fn persist_blob(&self, data: &[u8]) -> Result<BlobRef, PersistenceError> {
        let mut blob_refs = self.persist_blobs(&[data])?;
        blob_refs.pop().ok_or_else(|| {
            PersistenceError::SchemaInvariant(
                "single blob persistence returned no blob reference".to_owned(),
            )
        })
    }

    pub fn persist_blobs(&self, data: &[&[u8]]) -> Result<Vec<BlobRef>, PersistenceError> {
        let blobs = data
            .iter()
            .map(|bytes| {
                let hash = hex::encode(sha2::Sha256::digest(bytes));
                (
                    BlobRef::new(format!("blob:sha256:{hash}")),
                    self.blob_dir.join(&hash),
                    *bytes,
                )
            })
            .collect::<Vec<_>>();

        let unique_files = blobs
            .iter()
            .map(|(_, path, bytes)| (path.clone(), *bytes))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .filter(|(path, _)| !path.exists())
            .collect::<Vec<_>>();
        let workers = std::thread::available_parallelism()
            .map_or(1, std::num::NonZeroUsize::get)
            .min(8)
            .min(unique_files.len().max(1));
        let chunk_size = unique_files.len().div_ceil(workers);
        std::thread::scope(|scope| -> Result<(), PersistenceError> {
            let handles = unique_files
                .chunks(chunk_size.max(1))
                .map(|chunk| {
                    scope.spawn(move || -> Result<(), std::io::Error> {
                        for (path, bytes) in chunk {
                            if !path.exists() {
                                fs::write(path, bytes)?;
                            }
                        }
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().map_err(|_| {
                    PersistenceError::SchemaInvariant("parallel blob writer panicked".to_owned())
                })??;
            }
            Ok(())
        })?;

        let transaction = self.conn.unchecked_transaction()?;
        {
            let mut statement = transaction.prepare_cached(
                "INSERT OR IGNORE INTO blobs (blob_ref, file_name) VALUES (?1, ?2)",
            )?;
            for (blob_ref, path, _) in &blobs {
                let file_name = path
                    .file_name()
                    .and_then(std::ffi::OsStr::to_str)
                    .ok_or_else(|| {
                        PersistenceError::SchemaInvariant("blob file name is not UTF-8".to_owned())
                    })?;
                statement.execute(params![blob_ref.as_str(), file_name])?;
            }
        }
        transaction.commit()?;
        Ok(blobs.into_iter().map(|(blob_ref, _, _)| blob_ref).collect())
    }

    pub fn load_blobs(&self) -> Result<Vec<Vec<u8>>, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare("SELECT file_name FROM blobs ORDER BY file_name")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut blobs = Vec::new();
        for row in rows {
            blobs.push(fs::read(self.blob_dir.join(row?))?);
        }
        Ok(blobs)
    }

    pub fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, PersistenceError> {
        if limit == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "observation page limit must be greater than zero".to_owned(),
            ));
        }
        let mut stmt = self.conn.prepare(
            "SELECT leaf_id, append_seq, observation_json
             FROM observations
             WHERE append_seq > ?1
             ORDER BY append_seq
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after_append_seq, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (leaf_id, append_seq, json) = row?;
            result.push(StoredObservation {
                leaf_id,
                append_seq,
                observation: serde_json::from_str(&json)?,
            });
        }
        Ok(result)
    }

    pub fn observations_for_leaf_after(
        &self,
        leaf_id: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<StoredObservation>, PersistenceError> {
        if limit == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "leaf tail limit must be greater than zero".to_owned(),
            ));
        }
        let mut stmt = self.conn.prepare(
            "SELECT leaf_id, append_seq, observation_json
             FROM observations
             WHERE leaf_id = ?1 AND append_seq > ?2
             ORDER BY append_seq
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![leaf_id, after_append_seq, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (leaf_id, append_seq, json) = row?;
            result.push(StoredObservation {
                leaf_id,
                append_seq,
                observation: serde_json::from_str(&json)?,
            });
        }
        Ok(result)
    }

    pub fn observation_by_id(
        &self,
        id: &ObservationId,
    ) -> Result<Option<StoredObservation>, PersistenceError> {
        let row = self
            .conn
            .query_row(
                "SELECT leaf_id, append_seq, observation_json
                 FROM observations WHERE id = ?1",
                [id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(leaf_id, append_seq, json)| {
            Ok(StoredObservation {
                leaf_id,
                append_seq,
                observation: serde_json::from_str(&json)?,
            })
        })
        .transpose()
    }

    pub fn observations_for_privacy_key(
        &self,
        privacy_key: &str,
    ) -> Result<Vec<StoredObservation>, PersistenceError> {
        if privacy_key.trim().is_empty() {
            return Err(PersistenceError::SchemaInvariant(
                "privacy key must not be blank".to_owned(),
            ));
        }
        let mut statement = self.conn.prepare(
            "SELECT reverse_index.append_seq, reverse_index.observation_id, observations.leaf_id,
                    observations.observation_json
             FROM observation_privacy_keys reverse_index
             JOIN observations ON observations.id = reverse_index.observation_id
             WHERE reverse_index.privacy_key = ?1
             ORDER BY reverse_index.append_seq",
        )?;
        let rows = statement.query_map([privacy_key], |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        rows.map(|row| {
            let (append_seq, observation_id, leaf_id, json) = row?;
            let observation: Observation = serde_json::from_str(&json)?;
            if observation.id.as_str() != observation_id {
                return Err(PersistenceError::SchemaInvariant(format!(
                    "privacy index observation id {} disagrees with payload {}",
                    observation_id,
                    observation.id.as_str()
                )));
            }
            Ok(StoredObservation {
                leaf_id,
                append_seq,
                observation,
            })
        })
        .collect()
    }

    pub fn leaf_positions(&self) -> Result<Vec<LeafPosition>, PersistenceError> {
        let tree = self.partition_tree_snapshot()?;
        let mut positions = Vec::new();
        for leaf_id in tree.current_leaf_ids() {
            let append_seq = self.conn.query_row(
                "SELECT COALESCE(MAX(append_seq), 0) FROM observations WHERE leaf_id = ?1",
                [&leaf_id],
                |row| row.get::<_, u64>(0),
            )?;
            positions.push(LeafPosition {
                leaf_id,
                append_seq,
            });
        }
        Ok(positions)
    }

    pub fn split_leaf_if_capacity(&self, capacity: usize) -> Result<bool, PersistenceError> {
        if capacity == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "leaf capacity must be greater than zero".to_owned(),
            ));
        }
        let tree = self.partition_tree_snapshot()?;
        for parent_leaf_id in tree.current_leaf_ids() {
            let mut stmt = self.conn.prepare(
                "SELECT id, observation_json FROM observations
                 WHERE leaf_id = ?1 ORDER BY append_seq",
            )?;
            let rows = stmt.query_map([&parent_leaf_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut observations = Vec::new();
            let mut routed = Vec::new();
            for row in rows {
                let (id, json) = row?;
                let observation: Observation = serde_json::from_str(&json)?;
                let routing_key =
                    routing_key_from_observation_for_order(self.routing_key_order, &observation)
                        .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
                routed.push(RoutedObservation {
                    observation_id: id,
                    routing_key,
                });
                observations.push(observation);
            }
            if observations.len() < capacity {
                continue;
            }

            let left = format!("lake:{}", uuid::Uuid::now_v7());
            let right = format!("lake:{}", uuid::Uuid::now_v7());
            let Some(plan) = plan_capacity_split(&parent_leaf_id, &routed, capacity, &left, &right)
                .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?
            else {
                continue;
            };

            let transaction = self.conn.unchecked_transaction()?;
            let prepare_json = split_prepare_event_json(&parent_leaf_id, &left, &right)
                .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
            transaction.execute(
                "INSERT INTO partition_log (
                    event_type, parent_leaf_id, left_child_leaf_id, right_child_leaf_id,
                    reason, control_timestamp, event_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    PARTITION_EVENT_SPLIT_PREPARE,
                    parent_leaf_id,
                    left,
                    right,
                    PARTITION_SPLIT_REASON_CAPACITY,
                    chrono::Utc::now().to_rfc3339(),
                    prepare_json,
                ],
            )?;

            for target in &plan.rehome_targets {
                transaction.execute(
                    "UPDATE observations SET leaf_id = ?1 WHERE id = ?2 AND leaf_id = ?3",
                    params![
                        target.target_leaf_id,
                        target.observation_id,
                        plan.parent_leaf_id
                    ],
                )?;
            }

            let commit_json = split_commit_event_json(
                &plan.parent_leaf_id,
                &plan.left_child_leaf_id,
                &plan.right_child_leaf_id,
                plan.bit_index,
            )
            .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
            let next_tree = self.partition_tree_after_event(&commit_json)?;
            transaction.execute(
                "INSERT INTO partition_log (
                    event_type, parent_leaf_id, left_child_leaf_id, right_child_leaf_id,
                    bit_index, reason, control_timestamp, event_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    PARTITION_EVENT_SPLIT_COMMIT,
                    plan.parent_leaf_id,
                    plan.left_child_leaf_id,
                    plan.right_child_leaf_id,
                    i64::from(plan.bit_index),
                    PARTITION_SPLIT_REASON_CAPACITY,
                    chrono::Utc::now().to_rfc3339(),
                    commit_json,
                ],
            )?;
            transaction.commit()?;
            self.partition_tree.store(Some(Arc::new(next_tree)));
            return Ok(true);
        }
        Ok(false)
    }

    pub fn blue_green_migrate<F>(
        &self,
        new_routing_keyspec_version: &str,
        new_identity_keyspec_version: &str,
        mut transform: F,
    ) -> Result<(), PersistenceError>
    where
        F: FnMut(&Observation) -> Result<BlueGreenTransform, PersistenceError>,
    {
        if new_routing_keyspec_version == lethe_runtime::runtime::partition::ROUTING_KEYSPEC_VERSION
            && new_identity_keyspec_version
                == lethe_runtime::runtime::partition::IDENTITY_KEYSPEC_VERSION
        {
            return Err(PersistenceError::SchemaInvariant(
                "blue/green migration requires a changed keyspec version".to_owned(),
            ));
        }

        let old_partition_log = {
            let mut stmt = self.conn.prepare(
                "SELECT event_seq, event_type, event_json
                 FROM partition_log ORDER BY event_seq",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(serde_json::json!({
                    "event_seq": row.get::<_, i64>(0)?,
                    "event_type": row.get::<_, String>(1)?,
                    "event_json": row.get::<_, String>(2)?,
                }))
            })?;
            let mut events = Vec::new();
            for row in rows {
                events.push(row?);
            }
            serde_json::to_string(&events)?
        };

        let mut observations = Vec::new();
        let mut cursor = 0u64;
        loop {
            let page = self.observation_page(cursor, 512)?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|row| row.append_seq).unwrap_or(cursor);
            observations.extend(page);
        }

        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute_batch(
            "
            DROP TABLE IF EXISTS observations_green;
            DROP TABLE IF EXISTS partition_log_green;
            CREATE TABLE observations_green (
                append_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                leaf_id TEXT NOT NULL CHECK (leaf_id LIKE 'lake:%'),
                routing_key TEXT NOT NULL,
                identity_key TEXT NOT NULL,
                canonical_json_sha256 TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                observation_json TEXT NOT NULL,
                UNIQUE (leaf_id, identity_key)
            );
            CREATE TABLE partition_log_green (
                event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                leaf_id TEXT,
                parent_leaf_id TEXT,
                left_child_leaf_id TEXT,
                right_child_leaf_id TEXT,
                bit_index INTEGER,
                reason TEXT,
                routing_keyspec_json TEXT,
                identity_keyspec_json TEXT,
                control_timestamp TEXT,
                event_json TEXT NOT NULL
            );
            ",
        )?;

        let root_leaf_id = format!("lake:{}", uuid::Uuid::now_v7());
        let initialize_json = serde_json::to_string(
            &lethe_runtime::runtime::partition::InitializePartitionEvent {
                root_leaf_id: root_leaf_id.clone(),
                routing_keyspec_version: new_routing_keyspec_version.to_owned(),
                identity_keyspec_version: new_identity_keyspec_version.to_owned(),
            },
        )?;
        transaction.execute(
            "INSERT INTO partition_log_green (
                event_type, leaf_id, routing_keyspec_json, identity_keyspec_json,
                control_timestamp, event_json
             ) VALUES ('initialize', ?1, ?2, ?3, ?4, ?5)",
            params![
                root_leaf_id,
                serde_json::json!({"version": new_routing_keyspec_version}).to_string(),
                serde_json::json!({"version": new_identity_keyspec_version}).to_string(),
                chrono::Utc::now().to_rfc3339(),
                initialize_json,
            ],
        )?;

        for stored in observations {
            let transformed = transform(&stored.observation)?;
            let mut observation = stored.observation;
            observation.idempotency_key = transformed.identity_key.clone();
            let mut meta = observation.meta.as_object().cloned().unwrap_or_default();
            meta.insert(
                CANONICAL_JSON_META_KEY.to_owned(),
                serde_json::Value::String(transformed.canonical_json.clone()),
            );
            observation.meta = serde_json::Value::Object(meta);
            transaction.execute(
                "INSERT INTO observations_green (
                    id, leaf_id, routing_key, identity_key, canonical_json_sha256,
                    recorded_at, observation_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    observation.id.as_str(),
                    root_leaf_id,
                    transformed.routing_key,
                    transformed.identity_key.as_str(),
                    canonical_json_sha256(&transformed.canonical_json),
                    observation.recorded_at.to_rfc3339(),
                    serde_json::to_string(&observation)?,
                ],
            )?;
        }

        transaction.execute(
            "INSERT INTO keyspec_history (
                migration_id, routing_keyspec_version, identity_keyspec_version,
                partition_log_json, retired_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                format!("migration:{}", uuid::Uuid::now_v7()),
                lethe_runtime::runtime::partition::ROUTING_KEYSPEC_VERSION,
                lethe_runtime::runtime::partition::IDENTITY_KEYSPEC_VERSION,
                old_partition_log,
                chrono::Utc::now().to_rfc3339(),
            ],
        )?;

        transaction.execute_batch(
            "
            DROP TRIGGER partition_log_no_update;
            DROP TRIGGER partition_log_no_delete;
            DROP INDEX observations_leaf_append;
            DROP INDEX partition_log_single_initialize;
            DROP TABLE observations;
            DROP TABLE partition_log;
            ALTER TABLE observations_green RENAME TO observations;
            ALTER TABLE partition_log_green RENAME TO partition_log;
            CREATE INDEX observations_leaf_append ON observations(leaf_id, append_seq);
            CREATE UNIQUE INDEX partition_log_single_initialize
                ON partition_log(event_type) WHERE event_type = 'initialize';
            CREATE TRIGGER partition_log_no_update
            BEFORE UPDATE ON partition_log
            BEGIN
                SELECT RAISE(ABORT, 'partition_log is append-only');
            END;
            CREATE TRIGGER partition_log_no_delete
            BEFORE DELETE ON partition_log
            BEGIN
                SELECT RAISE(ABORT, 'partition_log is append-only');
            END;
            ",
        )?;
        transaction.execute("DELETE FROM observation_privacy_keys", [])?;
        let rebuilt_privacy_rows = {
            let mut statement = transaction.prepare(
                "SELECT id, append_seq, observation_json
                 FROM observations ORDER BY append_seq",
            )?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (observation_id, append_seq, json) in rebuilt_privacy_rows {
            let observation: Observation = serde_json::from_str(&json)?;
            for privacy_key in observation_privacy_keys(&observation) {
                transaction.execute(
                    "INSERT INTO observation_privacy_keys (
                        privacy_key, observation_id, append_seq
                     ) VALUES (?1, ?2, ?3)",
                    params![privacy_key, observation_id, append_seq],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn append_split_prepare(
        &self,
        parent_leaf_id: &str,
        left_child_leaf_id: &str,
        right_child_leaf_id: &str,
    ) -> Result<i64, PersistenceError> {
        let event_json =
            split_prepare_event_json(parent_leaf_id, left_child_leaf_id, right_child_leaf_id)
                .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
        self.conn.execute(
            "INSERT INTO partition_log (
                event_type,
                parent_leaf_id,
                left_child_leaf_id,
                right_child_leaf_id,
                reason,
                control_timestamp,
                event_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                PARTITION_EVENT_SPLIT_PREPARE,
                parent_leaf_id,
                left_child_leaf_id,
                right_child_leaf_id,
                PARTITION_SPLIT_REASON_CAPACITY,
                chrono::Utc::now().to_rfc3339(),
                event_json,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn append_split_commit(
        &self,
        parent_leaf_id: &str,
        left_child_leaf_id: &str,
        right_child_leaf_id: &str,
        bit_index: u32,
    ) -> Result<i64, PersistenceError> {
        let event_json = split_commit_event_json(
            parent_leaf_id,
            left_child_leaf_id,
            right_child_leaf_id,
            bit_index,
        )
        .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
        let next_tree = self.partition_tree_after_event(&event_json)?;
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO partition_log (
                event_type,
                parent_leaf_id,
                left_child_leaf_id,
                right_child_leaf_id,
                bit_index,
                reason,
                control_timestamp,
                event_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                PARTITION_EVENT_SPLIT_COMMIT,
                parent_leaf_id,
                left_child_leaf_id,
                right_child_leaf_id,
                i64::from(bit_index),
                PARTITION_SPLIT_REASON_CAPACITY,
                chrono::Utc::now().to_rfc3339(),
                event_json,
            ],
        )?;
        let event_seq = transaction.last_insert_rowid();
        transaction.commit()?;
        self.partition_tree.store(Some(Arc::new(next_tree)));
        Ok(event_seq)
    }

    pub fn append_failover(
        &self,
        leaf_id: &str,
        failover_id: &str,
    ) -> Result<i64, PersistenceError> {
        let event_json = failover_event_json(leaf_id, failover_id)
            .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
        self.conn.execute(
            "INSERT INTO partition_log (
                event_type,
                leaf_id,
                control_timestamp,
                event_json
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                PARTITION_EVENT_FAILOVER,
                leaf_id,
                chrono::Utc::now().to_rfc3339(),
                event_json,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn append_recover(
        &self,
        leaf_id: &str,
        failover_id: &str,
    ) -> Result<i64, PersistenceError> {
        let event_json = recover_event_json(leaf_id, failover_id)
            .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
        self.conn.execute(
            "INSERT INTO partition_log (
                event_type,
                leaf_id,
                control_timestamp,
                event_json
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                PARTITION_EVENT_RECOVER,
                leaf_id,
                chrono::Utc::now().to_rfc3339(),
                event_json,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn rebuild_partition_tree_from_log(&self) -> Result<PartitionTree, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare("SELECT event_type, event_json FROM partition_log ORDER BY event_seq")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut events = Vec::new();
        for row in rows {
            let (event_type, event_json) = row?;
            events.push(
                parse_partition_event(&event_type, &event_json)
                    .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?,
            );
        }

        PartitionTree::from_events(&events)
            .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))
    }

    fn partition_tree_snapshot(&self) -> Result<Arc<PartitionTree>, PersistenceError> {
        self.partition_tree.load_full().ok_or_else(|| {
            PersistenceError::SchemaInvariant(
                "partition tree snapshot is unavailable before schema initialization".to_owned(),
            )
        })
    }

    fn partition_tree_after_event(
        &self,
        event_json: &str,
    ) -> Result<PartitionTree, PersistenceError> {
        let event = parse_partition_event(PARTITION_EVENT_SPLIT_COMMIT, event_json)
            .map_err(|error| PersistenceError::SchemaInvariant(error.to_string()))?;
        let mut next = (*self.partition_tree_snapshot()?).clone();
        if let lethe_runtime::runtime::partition::PartitionLogEvent::SplitCommit(commit) = event {
            next.apply_split_commit(&commit)
                .map_err(|error| PersistenceError::SchemaInvariant(error.to_string()))?;
        }
        Ok(next)
    }

    pub fn load_partition_tree(&self) -> Result<PartitionTree, PersistenceError> {
        Ok((*self.partition_tree_snapshot()?).clone())
    }

    pub fn garbage_collect_orphan_blobs(&self) -> Result<usize, PersistenceError> {
        let mut stmt = self.conn.prepare("SELECT blob_ref FROM blobs")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut referenced = std::collections::HashSet::new();
        for row in rows {
            if let Some(hash) = row?.strip_prefix("blob:sha256:").map(ToOwned::to_owned) {
                referenced.insert(hash);
            }
        }

        let mut removed = 0usize;
        for entry in fs::read_dir(&self.blob_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if !referenced.contains(&name) {
                fs::remove_file(entry.path())?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub fn load_blob(&self, blob_ref: &BlobRef) -> Result<Option<Vec<u8>>, PersistenceError> {
        let path = self
            .conn
            .query_row(
                "SELECT file_name FROM blobs WHERE blob_ref = ?1",
                [blob_ref.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        path.map(|file_name| fs::read(self.blob_dir.join(file_name)))
            .transpose()
            .map_err(PersistenceError::from)
    }

    pub fn supplemental_page(
        &self,
        after_created_at: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SupplementalRecord>, PersistenceError> {
        if limit == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "supplemental page limit must be greater than zero".to_owned(),
            ));
        }
        let mut stmt = self.conn.prepare(
            "SELECT supplemental_json FROM supplementals
             WHERE (?1 IS NULL OR created_at > ?1)
             ORDER BY created_at, id
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after_created_at, limit], |row| {
            row.get::<_, String>(0)
        })?;
        let mut records = Vec::new();
        for row in rows {
            records.push(serde_json::from_str(&row?)?);
        }
        Ok(records)
    }

    pub fn supplemental_by_id(
        &self,
        id: &SupplementalId,
    ) -> Result<Option<SupplementalRecord>, PersistenceError> {
        let json = self
            .conn
            .query_row(
                "SELECT supplemental_json FROM supplementals WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        json.map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(PersistenceError::from)
    }

    pub fn materialize_projection(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        records: &serde_json::Value,
    ) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO projection_materializations (
                projection_id, records_json, materialized_at
             ) VALUES (?1, ?2, ?3)
             ON CONFLICT(projection_id) DO UPDATE SET
                records_json = excluded.records_json,
                materialized_at = excluded.materialized_at",
            params![projection.as_str(), "{}", chrono::Utc::now().to_rfc3339(),],
        )?;
        replace_manifest_fields(&transaction, projection, records)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn projection_records(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
    ) -> Result<Option<serde_json::Value>, PersistenceError> {
        let mut statement = self.conn.prepare(
            "SELECT field_key, value_json
             FROM projection_manifest_fields
             WHERE projection_id = ?1
             ORDER BY field_key",
        )?;
        let rows = statement.query_map([projection.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let fields = rows.collect::<Result<Vec<_>, _>>()?;
        if fields.is_empty() {
            return Ok(None);
        }
        let mut object = serde_json::Map::new();
        for (field_key, value_json) in fields {
            object.insert(field_key, serde_json::from_str(&value_json)?);
        }
        Ok(Some(serde_json::Value::Object(object)))
    }

    pub fn commit_projection_items(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        commit: &ProjectionItemCommit,
    ) -> Result<(), PersistenceError> {
        validate_projection_key(projection)?;
        commit
            .validate()
            .map_err(projection_item_validation_error)?;

        let manifest_json = "{}";
        let materialized_at = chrono::Utc::now().to_rfc3339();
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO projection_materializations (
                projection_id, records_json, materialized_at
             ) VALUES (?1, ?2, ?3)
             ON CONFLICT(projection_id) DO UPDATE SET
                records_json = excluded.records_json,
                materialized_at = excluded.materialized_at",
            params![projection.as_str(), manifest_json, materialized_at],
        )?;
        upsert_manifest_fields(&transaction, projection, manifest)?;

        match commit {
            ProjectionItemCommit::Replace { items } => {
                transaction.execute(
                    "DELETE FROM projection_materialization_items WHERE projection_id = ?1",
                    [projection.as_str()],
                )?;
                for item in items {
                    insert_projection_item(&transaction, projection, item)?;
                }
            }
            ProjectionItemCommit::Delta {
                inserts,
                updates,
                deletes,
            } => {
                apply_projection_item_delta(&transaction, projection, inserts, updates, deletes)?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn commit_supplemental_and_projection(
        &self,
        record: &SupplementalRecord,
        projection: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
    ) -> Result<(), PersistenceError> {
        self.commit_supplemental_and_projection_with_audit(
            record,
            projection,
            manifest,
            item_delta,
            &[],
        )
    }

    pub fn commit_supplemental_and_projection_with_audit(
        &self,
        record: &SupplementalRecord,
        projection: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> Result<(), PersistenceError> {
        validate_projection_key(projection)?;
        let ProjectionItemCommit::Delta {
            inserts,
            updates,
            deletes,
        } = item_delta
        else {
            return Err(PersistenceError::SchemaInvariant(
                "supplemental projection commit requires a projection item delta".to_owned(),
            ));
        };
        item_delta
            .validate()
            .map_err(projection_item_validation_error)?;

        let supplemental_json = serde_json::to_string(record)?;
        let manifest_json = "{}";
        let materialized_at = chrono::Utc::now().to_rfc3339();
        let transaction = self.conn.unchecked_transaction()?;
        insert_new_supplemental(&transaction, record, &supplemental_json)?;
        apply_projection_item_delta(&transaction, projection, inserts, updates, deletes)?;
        transaction.execute(
            "INSERT INTO projection_materializations (
                projection_id, records_json, materialized_at
             ) VALUES (?1, ?2, ?3)
             ON CONFLICT(projection_id) DO UPDATE SET
                records_json = excluded.records_json,
                materialized_at = excluded.materialized_at",
            params![projection.as_str(), manifest_json, materialized_at],
        )?;
        upsert_manifest_fields(&transaction, projection, manifest)?;
        for audit in audit_events {
            insert_audit_event(&transaction, audit)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn publish_projection_items_from_staging(
        &self,
        target: &lethe_core::domain::ProjectionRef,
        staging: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        expected_item_count: u64,
    ) -> Result<(), PersistenceError> {
        validate_projection_key(target)?;
        validate_projection_key(staging)?;
        if target == staging {
            return Err(PersistenceError::SchemaInvariant(
                "projection item staging projection must differ from target projection".to_owned(),
            ));
        }

        let expected_insert_count = usize::try_from(expected_item_count).map_err(|_| {
            PersistenceError::SchemaInvariant(
                "projection item staging expected count does not fit usize".to_owned(),
            )
        })?;
        let manifest_json = "{}";
        let materialized_at = chrono::Utc::now().to_rfc3339();
        let transaction = self.conn.unchecked_transaction()?;
        let staging_exists = transaction.query_row(
            "SELECT EXISTS (
                SELECT 1 FROM projection_materializations WHERE projection_id = ?1
             )",
            [staging.as_str()],
            |row| row.get::<_, bool>(0),
        )?;
        if !staging_exists {
            return Err(PersistenceError::SchemaInvariant(format!(
                "projection item staging projection {} does not exist",
                staging.as_str()
            )));
        }
        let actual_item_count = transaction.query_row(
            "SELECT COUNT(*) FROM projection_materialization_items WHERE projection_id = ?1",
            [staging.as_str()],
            |row| row.get::<_, u64>(0),
        )?;
        if actual_item_count != expected_item_count {
            return Err(PersistenceError::SchemaInvariant(format!(
                "projection item staging projection {} contains {actual_item_count} items, expected {expected_item_count}",
                staging.as_str()
            )));
        }

        transaction.execute(
            "DELETE FROM projection_materialization_items WHERE projection_id = ?1",
            [target.as_str()],
        )?;
        transaction.execute(
            "DELETE FROM projection_visible_blob_refs WHERE projection_id = ?1",
            [target.as_str()],
        )?;
        let inserted = transaction.execute(
            "INSERT INTO projection_materialization_items (
                projection_id, item_key, owner_key, sort_key, value_json
             )
             SELECT ?1, item_key, owner_key, sort_key, value_json
             FROM projection_materialization_items
             WHERE projection_id = ?2",
            params![target.as_str(), staging.as_str()],
        )?;
        if inserted != expected_insert_count {
            return Err(PersistenceError::SchemaInvariant(format!(
                "projection item staging publish copied {inserted} items, expected {expected_item_count}"
            )));
        }
        transaction.execute(
            "INSERT INTO projection_visible_blob_refs (
                projection_id, item_key, blob_ref, owner_key, consent_scope, subject_key
             )
             SELECT ?1, item_key, blob_ref, owner_key, consent_scope, subject_key
             FROM projection_visible_blob_refs
             WHERE projection_id = ?2",
            params![target.as_str(), staging.as_str()],
        )?;
        transaction.execute(
            "INSERT INTO projection_materializations (
                projection_id, records_json, materialized_at
             ) VALUES (?1, ?2, ?3)
             ON CONFLICT(projection_id) DO UPDATE SET
                records_json = excluded.records_json,
                materialized_at = excluded.materialized_at",
            params![target.as_str(), manifest_json, materialized_at],
        )?;
        upsert_manifest_fields(&transaction, target, manifest)?;
        let deleted_staging_items = transaction.execute(
            "DELETE FROM projection_materialization_items WHERE projection_id = ?1",
            [staging.as_str()],
        )?;
        transaction.execute(
            "DELETE FROM projection_visible_blob_refs WHERE projection_id = ?1",
            [staging.as_str()],
        )?;
        if deleted_staging_items != expected_insert_count {
            return Err(PersistenceError::SchemaInvariant(format!(
                "projection item staging cleanup deleted {deleted_staging_items} items, expected {expected_item_count}"
            )));
        }
        let deleted_staging_manifest = transaction.execute(
            "DELETE FROM projection_materializations WHERE projection_id = ?1",
            [staging.as_str()],
        )?;
        if deleted_staging_manifest != 1 {
            return Err(PersistenceError::SchemaInvariant(format!(
                "projection item staging cleanup did not delete manifest {}",
                staging.as_str()
            )));
        }
        transaction.execute(
            "DELETE FROM projection_manifest_fields WHERE projection_id = ?1",
            [staging.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn projection_item_by_key(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        item_key: &str,
    ) -> Result<Option<ProjectionItem>, PersistenceError> {
        validate_projection_key(projection)?;
        validate_item_key(item_key)?;
        let row = self
            .conn
            .query_row(
                "SELECT item_key, owner_key, sort_key, value_json
                 FROM projection_materialization_items
                 WHERE projection_id = ?1 AND item_key = ?2",
                params![projection.as_str(), item_key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(item_key, owner_key, sort_key, value_json)| {
            Ok(ProjectionItem {
                item_key,
                owner_key,
                sort_key,
                value: serde_json::from_str(&value_json)?,
            })
        })
        .transpose()
    }

    pub fn projection_items_by_owner(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        owner_key: &str,
    ) -> Result<Vec<ProjectionItem>, PersistenceError> {
        validate_projection_key(projection)?;
        validate_owner_key(owner_key)?;
        let mut statement = self.conn.prepare(
            "SELECT item_key, owner_key, sort_key, value_json
             FROM projection_materialization_items
             WHERE projection_id = ?1 AND owner_key = ?2
             ORDER BY sort_key ASC, item_key ASC",
        )?;
        let rows = statement.query_map(params![projection.as_str(), owner_key], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut items = Vec::new();
        for row in rows {
            let (item_key, owner_key, sort_key, value_json) = row?;
            items.push(ProjectionItem {
                item_key,
                owner_key,
                sort_key,
                value: serde_json::from_str(&value_json)?,
            });
        }
        Ok(items)
    }

    pub fn projection_items_page(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        owner_keys: &[String],
        item_key_prefix: Option<&str>,
        after_sort_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ProjectionItem>, PersistenceError> {
        validate_projection_key(projection)?;
        if owner_keys.is_empty() || limit == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "projection item page requires owners and a positive limit".to_owned(),
            ));
        }
        for owner_key in owner_keys {
            validate_owner_key(owner_key)?;
        }
        let placeholders = (0..owner_keys.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "SELECT item_key, owner_key, sort_key, value_json
             FROM projection_materialization_items
             WHERE projection_id = ?1 AND owner_key IN ({placeholders})"
        );
        let mut values = vec![rusqlite::types::Value::Text(projection.as_str().to_owned())];
        values.extend(owner_keys.iter().cloned().map(rusqlite::types::Value::Text));
        if let Some(prefix) = item_key_prefix {
            sql.push_str(" AND item_key LIKE ?");
            values.push(rusqlite::types::Value::Text(format!("{prefix}%")));
        }
        if let Some(after_sort_key) = after_sort_key {
            if let Some((sort_key, item_key)) = after_sort_key.rsplit_once('\u{001f}') {
                if sort_key.is_empty() || item_key.is_empty() {
                    return Err(PersistenceError::SchemaInvariant(
                        "projection item cursor boundary is invalid".to_owned(),
                    ));
                }
                sql.push_str(" AND (sort_key > ? OR (sort_key = ? AND item_key > ?))");
                values.push(rusqlite::types::Value::Text(sort_key.to_owned()));
                values.push(rusqlite::types::Value::Text(sort_key.to_owned()));
                values.push(rusqlite::types::Value::Text(item_key.to_owned()));
            } else {
                sql.push_str(" AND sort_key > ?");
                values.push(rusqlite::types::Value::Text(after_sort_key.to_owned()));
            }
        }
        sql.push_str(" ORDER BY sort_key ASC, item_key ASC LIMIT ?");
        values.push(rusqlite::types::Value::Integer(
            i64::try_from(limit).map_err(|_| {
                PersistenceError::SchemaInvariant("projection item limit overflow".to_owned())
            })?,
        ));
        let mut statement = self.conn.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        rows.map(|row| {
            let (item_key, owner_key, sort_key, value_json) = row?;
            Ok(ProjectionItem {
                item_key,
                owner_key,
                sort_key,
                value: serde_json::from_str(&value_json)?,
            })
        })
        .collect()
    }

    pub fn projection_blob_ref_visible(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        blob_ref: &BlobRef,
    ) -> Result<bool, PersistenceError> {
        validate_projection_key(projection)?;
        Ok(self.conn.query_row(
            "SELECT EXISTS (
                 SELECT 1 FROM projection_visible_blob_refs
                 WHERE projection_id = ?1 AND blob_ref = ?2
             )",
            params![projection.as_str(), blob_ref.as_str()],
            |row| row.get(0),
        )?)
    }

    pub fn projection_item_count_by_owner(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        owner_key: &str,
    ) -> Result<u64, PersistenceError> {
        validate_projection_key(projection)?;
        validate_owner_key(owner_key)?;
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM projection_materialization_items
                 WHERE projection_id = ?1 AND owner_key = ?2",
                params![projection.as_str(), owner_key],
                |row| row.get(0),
            )
            .map_err(PersistenceError::from)
    }

    pub fn projection_item_count(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
    ) -> Result<u64, PersistenceError> {
        validate_projection_key(projection)?;
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM projection_materialization_items
                 WHERE projection_id = ?1",
                [projection.as_str()],
                |row| row.get(0),
            )
            .map_err(PersistenceError::from)
    }

    pub fn projection_leaf_watermark(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        leaf_id: &str,
    ) -> Result<ProjectionLeafWatermark, PersistenceError> {
        let existing = self
            .conn
            .query_row(
                "SELECT append_seq, status FROM projection_leaf_watermarks
                 WHERE projection_id = ?1 AND leaf_id = ?2",
                params![projection.as_str(), leaf_id],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let (append_seq, status) = existing.unwrap_or((0, "success".to_owned()));
        Ok(ProjectionLeafWatermark {
            projection_id: projection.clone(),
            leaf_id: leaf_id.to_owned(),
            append_seq,
            status,
        })
    }

    pub fn commit_projection_leaf_watermark(
        &self,
        watermark: &ProjectionLeafWatermark,
    ) -> Result<(), PersistenceError> {
        let current =
            self.projection_leaf_watermark(&watermark.projection_id, &watermark.leaf_id)?;
        if watermark.append_seq < current.append_seq {
            return Err(PersistenceError::SchemaInvariant(format!(
                "projection leaf watermark cannot decrease: {} -> {}",
                current.append_seq, watermark.append_seq
            )));
        }
        self.conn.execute(
            "INSERT INTO projection_leaf_watermarks (
                projection_id, leaf_id, append_seq, status, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(projection_id, leaf_id) DO UPDATE SET
                append_seq = excluded.append_seq,
                status = excluded.status,
                updated_at = excluded.updated_at",
            params![
                watermark.projection_id.as_str(),
                watermark.leaf_id,
                watermark.append_seq,
                watermark.status,
                chrono::Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn deep_check(&self) -> Result<(), PersistenceError> {
        let integrity = self
            .conn
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))?;
        if integrity != "ok" {
            return Err(PersistenceError::SchemaInvariant(format!(
                "SQLite quick_check failed: {integrity}"
            )));
        }
        self.partition_tree_snapshot()?;
        Ok(())
    }

    pub fn put_encrypted_secret(
        &self,
        secret_ref: &str,
        plaintext: &[u8],
    ) -> Result<(), PersistenceError> {
        if secret_ref.trim().is_empty() || plaintext.is_empty() {
            return Err(PersistenceError::SchemaInvariant(
                "secret_ref and plaintext must not be empty".to_owned(),
            ));
        }
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.secret_encryption_key));
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher.encrypt(&nonce, plaintext).map_err(|_| {
            PersistenceError::SchemaInvariant("secret encryption failed".to_owned())
        })?;
        self.conn.execute(
            "INSERT INTO encrypted_secrets (secret_ref, nonce, ciphertext, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(secret_ref) DO UPDATE SET
                nonce = excluded.nonce,
                ciphertext = excluded.ciphertext,
                updated_at = excluded.updated_at",
            params![
                secret_ref,
                nonce.as_slice(),
                ciphertext,
                chrono::Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn get_encrypted_secret(
        &self,
        secret_ref: &str,
    ) -> Result<Option<Vec<u8>>, PersistenceError> {
        let encrypted = self
            .conn
            .query_row(
                "SELECT nonce, ciphertext FROM encrypted_secrets WHERE secret_ref = ?1",
                [secret_ref],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?;
        let Some((nonce, ciphertext)) = encrypted else {
            return Ok(None);
        };
        let nonce: [u8; 12] = nonce.try_into().map_err(|_| {
            PersistenceError::SchemaInvariant("stored secret nonce has invalid length".to_owned())
        })?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.secret_encryption_key));
        cipher
            .decrypt((&nonce).into(), ciphertext.as_ref())
            .map(Some)
            .map_err(|_| PersistenceError::SchemaInvariant("secret decryption failed".to_owned()))
    }

    pub fn record_dead_letter(&self, source: &str, reason: &str) -> Result<(), PersistenceError> {
        self.conn.execute(
            "INSERT INTO dead_letters (
                source_instance, item_key, reason, payload_json, created_at
             ) VALUES (?1, ?2, ?3, NULL, ?4)",
            params![source, source, reason, chrono::Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn record_audit_event(
        &self,
        id: &str,
        timestamp: &str,
        actor: &str,
        event_json: &str,
    ) -> Result<(), PersistenceError> {
        let audit = lethe_storage_api::AuditEventRecord {
            id: id.to_owned(),
            timestamp: timestamp.to_owned(),
            actor: actor.to_owned(),
            event_json: event_json.to_owned(),
        };
        insert_audit_event_connection(&self.conn, &audit)?;
        Ok(())
    }

    pub fn audit_event_page(
        &self,
        after: Option<&lethe_storage_api::AuditEventCursor>,
        limit: usize,
    ) -> Result<Vec<lethe_storage_api::AuditEventRecord>, PersistenceError> {
        if limit == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "audit event page limit must be greater than zero".to_owned(),
            ));
        }
        let (after_timestamp, after_id) = after.map_or((None, None), |cursor| {
            (Some(cursor.timestamp.as_str()), Some(cursor.id.as_str()))
        });
        let mut statement = self.conn.prepare(
            "SELECT id, timestamp, actor, event_json
             FROM audit_events
             WHERE ?1 IS NULL
                OR timestamp > ?1
                OR (timestamp = ?1 AND id > ?2)
             ORDER BY timestamp, id
             LIMIT ?3",
        )?;
        let rows = statement.query_map(params![after_timestamp, after_id, limit], |row| {
            Ok(lethe_storage_api::AuditEventRecord {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                actor: row.get(2)?,
                event_json: row.get(3)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::from)
    }

    pub fn record_sync_metrics(
        &self,
        source: &str,
        metrics: &SyncMetricRecord,
    ) -> Result<(), PersistenceError> {
        self.record_sync_state(
            source,
            &PersistedSyncState {
                metrics: metrics.clone(),
                completed_at: chrono::Utc::now(),
                error: None,
            },
        )
    }

    pub fn record_sync_state(
        &self,
        source: &str,
        state: &PersistedSyncState,
    ) -> Result<(), PersistenceError> {
        self.conn.execute(
            "INSERT INTO sync_metrics (
                source_instance, fetched, ingested, skipped, failed,
                quarantined, latency_ms, last_error, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(source_instance) DO UPDATE SET
                fetched = excluded.fetched,
                ingested = excluded.ingested,
                skipped = excluded.skipped,
                failed = excluded.failed,
                quarantined = excluded.quarantined,
                latency_ms = excluded.latency_ms,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at",
            params![
                source,
                state.metrics.fetched,
                state.metrics.ingested,
                state.metrics.skipped,
                state.metrics.failed,
                state.metrics.quarantined,
                state.metrics.latency_ms,
                state.error,
                state.completed_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn load_sync_state(
        &self,
        source: &str,
    ) -> Result<Option<PersistedSyncState>, PersistenceError> {
        let row = self
            .conn
            .query_row(
                "SELECT fetched, ingested, skipped, failed, quarantined,
                        latency_ms, last_error, updated_at
                 FROM sync_metrics WHERE source_instance = ?1",
                [source],
                |row| {
                    Ok((
                        SyncMetricRecord {
                            fetched: row.get(0)?,
                            ingested: row.get(1)?,
                            skipped: row.get(2)?,
                            failed: row.get(3)?,
                            quarantined: row.get(4)?,
                            latency_ms: row.get(5)?,
                        },
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(metrics, error, completed_at)| {
            let completed_at = completed_at
                .parse::<chrono::DateTime<chrono::Utc>>()
                .map_err(|parse_error| {
                    PersistenceError::SchemaInvariant(format!(
                        "sync_metrics updated_at is invalid: {parse_error}"
                    ))
                })?;
            Ok(PersistedSyncState {
                metrics,
                completed_at,
                error,
            })
        })
        .transpose()
    }

    pub fn apply_retention(&self, retention_days: u32) -> Result<usize, PersistenceError> {
        if retention_days == 0 {
            return Err(PersistenceError::SchemaInvariant(
                "retention_days must be positive".to_owned(),
            ));
        }
        let modifier = format!("-{retention_days} days");
        let dead_letters = self.conn.execute(
            "DELETE FROM dead_letters
             WHERE datetime(created_at) < datetime('now', ?1)",
            [&modifier],
        )?;
        let audits = self.conn.execute(
            "DELETE FROM audit_events
             WHERE datetime(timestamp) < datetime('now', ?1)",
            [&modifier],
        )?;
        Ok(dead_letters + audits)
    }
}

impl SqliteOperationalEventStore {
    pub fn open(
        data_space_id: DataSpaceId,
        database_path: &Path,
        blob_dir: &Path,
        secret_encryption_key: &[u8; 32],
    ) -> Result<Self, PersistenceError> {
        if data_space_id.as_str().trim().is_empty() {
            return Err(PersistenceError::SchemaInvariant(
                "data_space_id must not be blank".to_owned(),
            ));
        }
        let persistence = SqlitePersistence::open(database_path, blob_dir, secret_encryption_key)?;
        let existing = persistence
            .conn
            .query_row(
                "SELECT data_space_id FROM operational_data_space WHERE singleton = 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        match existing {
            Some(existing) if existing != data_space_id.as_str() => {
                return Err(PersistenceError::SchemaInvariant(format!(
                    "sqlite Lake is pinned to data space {existing}, not {data_space_id}"
                )));
            }
            Some(_) => {}
            None => {
                persistence.conn.execute(
                    "INSERT INTO operational_data_space (singleton, data_space_id)
                     VALUES (1, ?1)",
                    [data_space_id.as_str()],
                )?;
            }
        }
        persistence.conn.execute(
            "INSERT OR IGNORE INTO operational_event_stats (
                data_space_id, event_count, max_cursor
             )
             SELECT ?1, COUNT(*), COALESCE(MAX(cursor), 0)
             FROM operational_events
             WHERE data_space_id = ?1",
            [data_space_id.as_str()],
        )?;
        Ok(Self {
            persistence,
            data_space_id,
        })
    }

    pub fn persistence(&self) -> &SqlitePersistence {
        &self.persistence
    }

    fn storage_error(error: impl std::fmt::Display) -> StorageError {
        StorageError::Backend(error.to_string())
    }

    fn append_operational_events_internal(
        &self,
        requests: &[OperationalAppendRequest],
        v2_admission: Option<(&str, Option<u64>)>,
    ) -> StorageResult<Vec<OperationalAppendOutcome>> {
        for request in requests {
            request.event.validate()?;
            if request.event.data_space_id != self.data_space_id {
                return Err(StorageError::Invariant(format!(
                    "event data space {} does not match sqlite Lake {}",
                    request.event.data_space_id, self.data_space_id
                )));
            }
            if request.event.stream_version != request.expected_stream_version + 1 {
                return Err(StorageError::Invariant(format!(
                    "event {} stream_version {} does not follow expected {}",
                    request.event.event_id,
                    request.event.stream_version,
                    request.expected_stream_version
                )));
            }
        }

        if v2_admission.is_some() {
            loop {
                let report = self.persistence.identity_bridge_apply_batch(16_384)?;
                if report.read_count == 0 {
                    break;
                }
            }
        }

        let tree = self
            .persistence
            .partition_tree_snapshot()
            .map_err(Self::storage_error)?;
        let transaction = self
            .persistence
            .conn
            .unchecked_transaction()
            .map_err(Self::storage_error)?;
        if let Some((source_instance_id, generation)) = v2_admission
            && let Err(error) = self.persistence.cutover_admit_transaction(
                &transaction,
                source_instance_id,
                CutoverApiVersion::V2,
                generation,
            )
        {
            transaction.commit().map_err(Self::storage_error)?;
            return Err(storage_error(error));
        }
        let mut outcomes = Vec::with_capacity(requests.len());
        let mut bridge_hits = 0_u64;
        let mut appended_ids = Vec::new();

        for request in requests {
            let event_json = serde_json::to_string(&request.event).map_err(Self::storage_error)?;
            let event_sha256 = hex::encode(sha2::Sha256::digest(event_json.as_bytes()));
            let idempotency_key = request.event.observation.idempotency_key.as_str();

            let duplicate = transaction
                .query_row(
                    "SELECT cursor, stream_version, event_sha256, event_id
                     FROM operational_events
                     WHERE data_space_id = ?1 AND idempotency_key = ?2",
                    params![self.data_space_id.as_str(), idempotency_key],
                    |row| {
                        Ok((
                            row.get::<_, u64>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .optional()
                .map_err(Self::storage_error)?;
            if let Some((cursor, stream_version, stored_sha256, stored_event_id)) = duplicate {
                if stored_sha256 != event_sha256
                    || stored_event_id != request.event.event_id.as_str()
                {
                    return Err(StorageError::OperationalIdempotencyCollision(
                        idempotency_key.to_owned(),
                    ));
                }
                outcomes.push(OperationalAppendOutcome::Duplicate {
                    cursor,
                    stream_version,
                });
                continue;
            }

            if let Some((source_instance_id, _)) = v2_admission {
                let (observation_outcome, bridge_hit) = self
                    .persistence
                    .append_v2_observation_for_operational_event(
                        &transaction,
                        &tree,
                        source_instance_id,
                        &request.event.observation,
                    )
                    .map_err(Self::storage_error)?;
                match observation_outcome {
                    DurableAppendOutcome::Duplicate(existing_id) => {
                        if bridge_hit {
                            bridge_hits = bridge_hits.checked_add(1).ok_or_else(|| {
                                StorageError::Invariant("bridge hit count overflow".to_owned())
                            })?;
                        }
                        let existing_event = transaction
                            .query_row(
                                "SELECT cursor, stream_version FROM operational_events
                                 WHERE observation_id = ?1",
                                [existing_id.as_str()],
                                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
                            )
                            .optional()
                            .map_err(Self::storage_error)?
                            .ok_or_else(|| {
                                StorageError::Invariant(format!(
                                    "bridge winner observation {existing_id} has no operational event"
                                ))
                            })?;
                        outcomes.push(OperationalAppendOutcome::Duplicate {
                            cursor: existing_event.0,
                            stream_version: existing_event.1,
                        });
                        continue;
                    }
                    DurableAppendOutcome::CanonicalCollision(existing_id) => {
                        return Err(StorageError::Invariant(format!(
                            "history v2 canonical collision with observation {existing_id}"
                        )));
                    }
                    DurableAppendOutcome::Appended(observation_id) => {
                        appended_ids.push(observation_id);
                    }
                }
            }

            let existing_event = transaction
                .query_row(
                    "SELECT cursor, event_sha256 FROM operational_events
                     WHERE event_id = ?1",
                    [request.event.event_id.as_str()],
                    |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(Self::storage_error)?;
            if let Some((cursor, stored_sha256)) = existing_event {
                if stored_sha256 != event_sha256 {
                    return Err(StorageError::OperationalEventIdCollision(
                        request.event.event_id.as_str().to_owned(),
                    ));
                }
                outcomes.push(OperationalAppendOutcome::Duplicate {
                    cursor,
                    stream_version: request.event.stream_version,
                });
                continue;
            }

            let actual = transaction
                .query_row(
                    "SELECT COALESCE(MAX(stream_version), 0)
                     FROM operational_events
                     WHERE data_space_id = ?1 AND stream_id = ?2",
                    params![self.data_space_id.as_str(), request.event.stream_id],
                    |row| row.get::<_, u64>(0),
                )
                .map_err(Self::storage_error)?;
            if actual != request.expected_stream_version {
                if v2_admission.is_some() {
                    return Err(StorageError::Invariant(format!(
                        "v2 history event stream {} conflicts: expected {}, actual {}",
                        request.event.stream_id, request.expected_stream_version, actual
                    )));
                }
                outcomes.push(OperationalAppendOutcome::VersionConflict {
                    expected: request.expected_stream_version,
                    actual,
                });
                continue;
            }

            if v2_admission.is_none() {
                let mut observation_outcomes = append_observations_in_transaction(
                    &transaction,
                    &tree,
                    self.persistence.routing_key_order,
                    std::slice::from_ref(&request.event.observation),
                )
                .map_err(Self::storage_error)?;
                match observation_outcomes.remove(0) {
                    DurableAppendOutcome::Appended(observation_id) => {
                        appended_ids.push(observation_id);
                    }
                    DurableAppendOutcome::Duplicate(observation_id) => {
                        return Err(StorageError::Invariant(format!(
                            "observation {observation_id} exists without its operational event"
                        )));
                    }
                    DurableAppendOutcome::CanonicalCollision(observation_id) => {
                        return Err(StorageError::Invariant(format!(
                            "operational observation identity collision with {observation_id}"
                        )));
                    }
                }
            }

            transaction
                .execute(
                    "INSERT INTO operational_events (
                        event_id, data_space_id, stream_id, stream_version,
                        idempotency_key, event_type, actor_id, causation_id,
                        correlation_id, occurred_at, observation_id,
                        event_sha256, event_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        request.event.event_id.as_str(),
                        self.data_space_id.as_str(),
                        request.event.stream_id,
                        request.event.stream_version,
                        idempotency_key,
                        request.event.event_type,
                        request.event.actor_id.as_deref(),
                        request.event.causation_id.as_ref().map(|id| id.as_str()),
                        request.event.correlation_id.as_deref(),
                        request.event.occurred_at.to_rfc3339(),
                        request.event.observation.id.as_str(),
                        event_sha256,
                        event_json,
                    ],
                )
                .map_err(Self::storage_error)?;
            let cursor = transaction.last_insert_rowid() as u64;
            transaction
                .execute(
                    "UPDATE operational_event_stats
                     SET event_count = event_count + 1,
                         max_cursor = MAX(max_cursor, ?2)
                     WHERE data_space_id = ?1",
                    params![self.data_space_id.as_str(), cursor],
                )
                .map_err(Self::storage_error)?;
            outcomes.push(OperationalAppendOutcome::Appended {
                cursor,
                stream_version: request.event.stream_version,
            });
        }

        if let Some((source_instance_id, _)) = v2_admission {
            SqlitePersistence::record_v2_append_metrics(
                &transaction,
                source_instance_id,
                bridge_hits,
                &appended_ids,
                "actor:history-importer",
                "first v2 history observation committed",
            )?;
        }
        transaction.commit().map_err(Self::storage_error)?;
        Ok(outcomes)
    }
}

impl OperationalEventStore for SqliteOperationalEventStore {
    fn data_space_id(&self) -> &DataSpaceId {
        &self.data_space_id
    }

    fn append_operational_events(
        &self,
        requests: &[OperationalAppendRequest],
    ) -> StorageResult<Vec<OperationalAppendOutcome>> {
        self.append_operational_events_internal(requests, None)
    }

    fn append_operational_events_v2_with_bridge(
        &self,
        source_instance_id: &str,
        generation: Option<u64>,
        requests: &[OperationalAppendRequest],
    ) -> StorageResult<Vec<OperationalAppendOutcome>> {
        if source_instance_id.trim().is_empty() {
            return Err(StorageError::Invariant(
                "v2 source_instance_id must not be blank".to_owned(),
            ));
        }
        if generation == Some(0) {
            return Err(StorageError::Invariant(
                "v2 admission generation must be positive".to_owned(),
            ));
        }
        self.append_operational_events_internal(requests, Some((source_instance_id, generation)))
    }

    fn operational_event_stats(&self) -> StorageResult<OperationalEventStats> {
        self.persistence
            .conn
            .query_row(
                "SELECT event_count, max_cursor
                 FROM operational_event_stats WHERE data_space_id = ?1",
                [self.data_space_id.as_str()],
                |row| {
                    Ok(OperationalEventStats {
                        count: row.get(0)?,
                        max_cursor: row.get(1)?,
                    })
                },
            )
            .map_err(Self::storage_error)
    }

    fn operational_event_page(
        &self,
        after_cursor: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredOperationalEvent>> {
        if limit == 0 {
            return Err(StorageError::Invariant(
                "operational event page limit must be positive".to_owned(),
            ));
        }
        let mut statement = self
            .persistence
            .conn
            .prepare(
                "SELECT cursor, event_json FROM operational_events
                 WHERE data_space_id = ?1 AND cursor > ?2
                 ORDER BY cursor LIMIT ?3",
            )
            .map_err(Self::storage_error)?;
        let rows = statement
            .query_map(
                params![self.data_space_id.as_str(), after_cursor, limit as u64],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(Self::storage_error)?;
        rows.map(|row| {
            let (cursor, event_json) = row.map_err(Self::storage_error)?;
            let event = serde_json::from_str(&event_json).map_err(Self::storage_error)?;
            Ok(StoredOperationalEvent { cursor, event })
        })
        .collect()
    }

    fn operational_events_by_filter(
        &self,
        filter: &OperationalEventFilter,
        after_cursor: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredOperationalEvent>> {
        if limit == 0 {
            return Err(StorageError::Invariant(
                "operational event page limit must be positive".to_owned(),
            ));
        }
        if filter
            .occurred_at_from
            .zip(filter.occurred_at_to)
            .is_some_and(|(from, to)| from > to)
        {
            return Err(StorageError::Invariant(
                "operational event occurred_at range is inverted".to_owned(),
            ));
        }
        let mut sql = String::from(
            "SELECT cursor, event_json FROM operational_events
             WHERE data_space_id = ? AND cursor > ?",
        );
        let mut values = vec![
            rusqlite::types::Value::Text(self.data_space_id.as_str().to_owned()),
            rusqlite::types::Value::Integer(i64::try_from(after_cursor).map_err(|_| {
                StorageError::Invariant("after_cursor does not fit SQLite INTEGER".to_owned())
            })?),
        ];
        if let Some(value) = &filter.correlation_id {
            require_operational_filter_value("correlation_id", value)?;
            sql.push_str(" AND correlation_id = ?");
            values.push(rusqlite::types::Value::Text(value.clone()));
        }
        if let Some(value) = &filter.causation_id {
            sql.push_str(" AND causation_id = ?");
            values.push(rusqlite::types::Value::Text(value.as_str().to_owned()));
        }
        if let Some(value) = &filter.event_type {
            require_operational_filter_value("event_type", value)?;
            sql.push_str(" AND event_type = ?");
            values.push(rusqlite::types::Value::Text(value.clone()));
        }
        if let Some(value) = &filter.stream_id {
            require_operational_filter_value("stream_id", value)?;
            sql.push_str(" AND stream_id = ?");
            values.push(rusqlite::types::Value::Text(value.clone()));
        }
        if let Some(value) = &filter.actor_id {
            require_operational_filter_value("actor_id", value)?;
            sql.push_str(" AND actor_id = ?");
            values.push(rusqlite::types::Value::Text(value.clone()));
        }
        if let Some(value) = filter.occurred_at_from {
            sql.push_str(" AND occurred_at >= ?");
            values.push(rusqlite::types::Value::Text(value.to_rfc3339()));
        }
        if let Some(value) = filter.occurred_at_to {
            sql.push_str(" AND occurred_at <= ?");
            values.push(rusqlite::types::Value::Text(value.to_rfc3339()));
        }
        sql.push_str(" ORDER BY cursor LIMIT ?");
        values.push(rusqlite::types::Value::Integer(
            i64::try_from(limit).map_err(|_| {
                StorageError::Invariant("limit does not fit SQLite INTEGER".to_owned())
            })?,
        ));

        let mut statement = self
            .persistence
            .conn
            .prepare(&sql)
            .map_err(Self::storage_error)?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(values), |row| {
                Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(Self::storage_error)?;
        rows.map(|row| {
            let (cursor, event_json) = row.map_err(Self::storage_error)?;
            let event = serde_json::from_str(&event_json).map_err(Self::storage_error)?;
            Ok(StoredOperationalEvent { cursor, event })
        })
        .collect()
    }

    fn operational_events_for_stream(
        &self,
        stream_id: &str,
        after_stream_version: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredOperationalEvent>> {
        if stream_id.trim().is_empty() || limit == 0 {
            return Err(StorageError::Invariant(
                "stream_id and a positive limit are required".to_owned(),
            ));
        }
        let mut statement = self
            .persistence
            .conn
            .prepare(
                "SELECT cursor, event_json FROM operational_events
                 WHERE data_space_id = ?1 AND stream_id = ?2 AND stream_version > ?3
                 ORDER BY stream_version LIMIT ?4",
            )
            .map_err(Self::storage_error)?;
        let rows = statement
            .query_map(
                params![
                    self.data_space_id.as_str(),
                    stream_id,
                    after_stream_version,
                    limit as u64
                ],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(Self::storage_error)?;
        rows.map(|row| {
            let (cursor, event_json) = row.map_err(Self::storage_error)?;
            let event = serde_json::from_str(&event_json).map_err(Self::storage_error)?;
            Ok(StoredOperationalEvent { cursor, event })
        })
        .collect()
    }

    fn operational_event_by_id(
        &self,
        event_id: &OperationalEventId,
    ) -> StorageResult<Option<StoredOperationalEvent>> {
        let row = self
            .persistence
            .conn
            .query_row(
                "SELECT cursor, event_json FROM operational_events
                 WHERE data_space_id = ?1 AND event_id = ?2",
                params![self.data_space_id.as_str(), event_id.as_str()],
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(Self::storage_error)?;
        row.map(|(cursor, event_json)| {
            let event = serde_json::from_str(&event_json).map_err(Self::storage_error)?;
            Ok(StoredOperationalEvent { cursor, event })
        })
        .transpose()
    }

    fn operational_stream_version(&self, stream_id: &str) -> StorageResult<u64> {
        if stream_id.trim().is_empty() {
            return Err(StorageError::Invariant(
                "stream_id must not be blank".to_owned(),
            ));
        }
        self.persistence
            .conn
            .query_row(
                "SELECT COALESCE(MAX(stream_version), 0) FROM operational_events
                 WHERE data_space_id = ?1 AND stream_id = ?2",
                params![self.data_space_id.as_str(), stream_id],
                |row| row.get(0),
            )
            .map_err(Self::storage_error)
    }
}

impl BlobStorePort for SqliteOperationalEventStore {
    fn put_blob(&self, data: &[u8], max_bytes: usize) -> StorageResult<BlobRef> {
        <SqlitePersistence as BlobStorePort>::put_blob(&self.persistence, data, max_bytes)
    }

    fn put_blobs(&self, data: &[&[u8]], max_bytes: usize) -> StorageResult<Vec<BlobRef>> {
        <SqlitePersistence as BlobStorePort>::put_blobs(&self.persistence, data, max_bytes)
    }

    fn get_blob(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>> {
        <SqlitePersistence as BlobStorePort>::get_blob(&self.persistence, blob_ref)
    }
}

fn append_observations_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    tree: &PartitionTree,
    routing_key_order: RoutingKeyOrder,
    observations: &[Observation],
) -> Result<Vec<DurableAppendOutcome>, PersistenceError> {
    let mut outcomes = Vec::with_capacity(observations.len());
    for observation in observations {
        let routing_key = routing_key_from_observation_for_order(routing_key_order, observation)
            .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
        let leaf_id = tree.route(&routing_key);
        let identity_key = &observation.idempotency_key;
        let canonical_json = observation
            .meta
            .get(CANONICAL_JSON_META_KEY)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                PersistenceError::SchemaInvariant(
                    "observation.meta.canonical_json is required for durable ingest".to_owned(),
                )
            })?;
        let canonical_json_sha256 = canonical_json_sha256(canonical_json);

        let existing = transaction
            .query_row(
                "SELECT r.observation_id, r.canonical_json_sha256, o.observation_json
                 FROM observation_identity_registry r
                 JOIN observations o ON o.id = r.observation_id
                 WHERE r.identity_key = ?1",
                [identity_key.as_str()],
                |row| {
                    Ok((
                        ObservationId::new(row.get::<_, String>(0)?),
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let mut existing = existing;
        if existing.is_none() {
            existing = transaction
                .query_row(
                    "SELECT id, canonical_json_sha256, observation_json
                     FROM observations
                     WHERE identity_key = ?1
                     ORDER BY append_seq
                     LIMIT 1",
                    [identity_key.as_str()],
                    |row| {
                        Ok((
                            ObservationId::new(row.get::<_, String>(0)?),
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?;
        }
        if let Some((existing_id, existing_hash, existing_json)) = existing {
            let same = existing_hash == canonical_json_sha256
                && canonical_json_from_observation_json(&existing_json)? == canonical_json;
            outcomes.push(if same {
                DurableAppendOutcome::Duplicate(existing_id)
            } else {
                DurableAppendOutcome::CanonicalCollision(existing_id)
            });
            continue;
        }

        transaction.execute(
            "INSERT INTO observation_identity_registry (
                identity_key, observation_id, canonical_json_sha256
             ) VALUES (?1, ?2, ?3)",
            params![
                identity_key.as_str(),
                observation.id.as_str(),
                canonical_json_sha256,
            ],
        )?;
        let json = serde_json::to_string(observation)?;
        transaction.execute(
            "INSERT INTO observations (
                id,
                leaf_id,
                routing_key,
                identity_key,
                canonical_json_sha256,
                recorded_at,
                observation_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                observation.id.as_str(),
                leaf_id,
                routing_key.encoded(),
                identity_key.as_str(),
                canonical_json_sha256,
                observation.recorded_at.to_rfc3339(),
                json,
            ],
        )?;
        let append_seq = transaction.last_insert_rowid() as u64;
        transaction.execute(
            "UPDATE observation_stats
             SET observation_count = observation_count + 1,
                 max_append_seq = MAX(max_append_seq, ?1)
             WHERE singleton = 1",
            [append_seq],
        )?;
        for privacy_key in observation_privacy_keys(observation) {
            transaction.execute(
                "INSERT INTO observation_privacy_keys (
                    privacy_key, observation_id, append_seq
                 ) VALUES (?1, ?2, ?3)",
                params![privacy_key, observation.id.as_str(), append_seq],
            )?;
        }
        outcomes.push(DurableAppendOutcome::Appended(observation.id.clone()));
    }
    Ok(outcomes)
}

fn validate_non_blank(field: &str, value: &str) -> Result<(), PersistenceError> {
    if value.trim().is_empty() {
        return Err(PersistenceError::SchemaInvariant(format!(
            "Slack thread {field} must not be blank"
        )));
    }
    Ok(())
}

fn validate_slack_thread_key(key: &SlackThreadKey) -> Result<(), PersistenceError> {
    validate_non_blank("source_instance", &key.source_instance)?;
    validate_non_blank("channel_id", &key.channel_id)?;
    validate_non_blank("thread_ts", &key.thread_ts)
}

fn upsert_slack_thread(
    transaction: &rusqlite::Transaction<'_>,
    key: &SlackThreadKey,
    observation_append_seq: u64,
) -> Result<(), PersistenceError> {
    validate_slack_thread_key(key)?;
    if observation_append_seq == 0 {
        return Err(PersistenceError::SchemaInvariant(
            "Slack thread discovery append sequence must be positive".to_owned(),
        ));
    }
    let generation = transaction.query_row(
        "SELECT poll_generation
         FROM slack_thread_catalog_state
         WHERE singleton = 1",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    transaction.execute(
        "INSERT INTO slack_thread_catalog (
            source_instance,
            channel_id,
            thread_ts,
            reply_cursor,
            active,
            next_poll_generation,
            discovered_append_seq
         ) VALUES (?1, ?2, ?3, ?3, 1, ?4, ?5)
         ON CONFLICT(source_instance, channel_id, thread_ts) DO UPDATE SET
            active = 1,
            next_poll_generation = MIN(
                slack_thread_catalog.next_poll_generation,
                excluded.next_poll_generation
            ),
            discovered_append_seq = MIN(
                slack_thread_catalog.discovered_append_seq,
                excluded.discovered_append_seq
            )",
        params![
            key.source_instance,
            key.channel_id,
            key.thread_ts,
            generation,
            observation_append_seq,
        ],
    )?;
    Ok(())
}

fn insert_audit_event(
    transaction: &rusqlite::Transaction<'_>,
    audit: &lethe_storage_api::AuditEventRecord,
) -> Result<(), PersistenceError> {
    transaction.execute(
        "INSERT INTO audit_events (id, timestamp, actor, event_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![audit.id, audit.timestamp, audit.actor, audit.event_json],
    )?;
    Ok(())
}

fn insert_audit_event_connection(
    connection: &Connection,
    audit: &lethe_storage_api::AuditEventRecord,
) -> Result<(), PersistenceError> {
    connection.execute(
        "INSERT INTO audit_events (id, timestamp, actor, event_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![audit.id, audit.timestamp, audit.actor, audit.event_json],
    )?;
    Ok(())
}

fn require_operational_filter_value(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        return Err(StorageError::Invariant(format!(
            "operational event {field} filter must not be blank"
        )));
    }
    Ok(())
}

fn manifest_fields(
    manifest: &serde_json::Value,
) -> Result<&serde_json::Map<String, serde_json::Value>, PersistenceError> {
    manifest.as_object().ok_or_else(|| {
        PersistenceError::SchemaInvariant("projection manifest must be a JSON object".to_owned())
    })
}

fn replace_manifest_fields(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    manifest: &serde_json::Value,
) -> Result<(), PersistenceError> {
    transaction.execute(
        "DELETE FROM projection_manifest_fields WHERE projection_id = ?1",
        [projection.as_str()],
    )?;
    upsert_manifest_fields(transaction, projection, manifest)
}

fn upsert_manifest_fields(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    manifest: &serde_json::Value,
) -> Result<(), PersistenceError> {
    let fields = manifest_fields(manifest)?;
    let mut existing = transaction
        .prepare("SELECT field_key FROM projection_manifest_fields WHERE projection_id = ?1")?;
    let existing_keys = existing
        .query_map([projection.as_str()], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(existing);
    for field_key in existing_keys {
        if !fields.contains_key(&field_key) {
            transaction.execute(
                "DELETE FROM projection_manifest_fields
                 WHERE projection_id = ?1 AND field_key = ?2",
                params![projection.as_str(), field_key],
            )?;
        }
    }
    for (field_key, field_value) in fields {
        transaction.execute(
            "INSERT INTO projection_manifest_fields (
                projection_id, field_key, value_json
             ) VALUES (?1, ?2, ?3)
             ON CONFLICT(projection_id, field_key) DO UPDATE SET
                value_json = excluded.value_json
             WHERE projection_manifest_fields.value_json IS NOT excluded.value_json",
            params![
                projection.as_str(),
                field_key,
                serde_json::to_string(field_value)?
            ],
        )?;
    }
    Ok(())
}

fn validate_projection_key(
    projection: &lethe_core::domain::ProjectionRef,
) -> Result<(), PersistenceError> {
    if projection.as_str().trim().is_empty() {
        return Err(PersistenceError::SchemaInvariant(
            "projection item projection_id must not be blank".to_owned(),
        ));
    }
    Ok(())
}

fn validate_owner_key(owner_key: &str) -> Result<(), PersistenceError> {
    if owner_key.trim().is_empty() {
        return Err(PersistenceError::SchemaInvariant(
            "projection item owner_key must not be blank".to_owned(),
        ));
    }
    Ok(())
}

fn validate_item_key(item_key: &str) -> Result<(), PersistenceError> {
    if item_key.trim().is_empty() {
        return Err(PersistenceError::SchemaInvariant(
            "projection item item_key must not be blank".to_owned(),
        ));
    }
    Ok(())
}

fn projection_item_validation_error(error: StorageError) -> PersistenceError {
    match error {
        StorageError::Invariant(message)
        | StorageError::Backend(message)
        | StorageError::OperationalIdempotencyCollision(message)
        | StorageError::OperationalEventIdCollision(message)
        | StorageError::CutoverAdmissionDenied(message)
        | StorageError::CutoverConflict(message)
        | StorageError::CutoverRollbackRefused(message) => {
            PersistenceError::SchemaInvariant(message)
        }
    }
}

fn insert_new_supplemental(
    transaction: &rusqlite::Transaction<'_>,
    record: &SupplementalRecord,
    supplemental_json: &str,
) -> Result<(), PersistenceError> {
    let inserted = transaction.execute(
        "INSERT INTO supplementals (id, created_at, supplemental_json)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(id) DO NOTHING",
        params![
            record.id.as_str(),
            record.created_at.to_rfc3339(),
            supplemental_json,
        ],
    )?;
    if inserted != 1 {
        return Err(PersistenceError::SchemaInvariant(format!(
            "supplemental append requires absent id {}",
            record.id.as_str()
        )));
    }
    Ok(())
}

fn apply_projection_item_delta(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    inserts: &[ProjectionItem],
    updates: &[ProjectionItem],
    deletes: &[String],
) -> Result<(), PersistenceError> {
    for item_key in deletes {
        delete_projection_item(transaction, projection, item_key)?;
    }
    for item in updates {
        update_projection_item(transaction, projection, item)?;
    }
    for item in inserts {
        insert_new_projection_item(transaction, projection, item)?;
    }
    Ok(())
}

fn insert_projection_item(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    item: &ProjectionItem,
) -> Result<(), PersistenceError> {
    transaction.execute(
        "INSERT INTO projection_materialization_items (
            projection_id, item_key, owner_key, sort_key, value_json
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            projection.as_str(),
            item.item_key,
            item.owner_key,
            item.sort_key,
            serde_json::to_string(&item.value)?,
        ],
    )?;
    replace_visible_blob_refs(transaction, projection, item)?;
    Ok(())
}

fn insert_new_projection_item(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    item: &ProjectionItem,
) -> Result<(), PersistenceError> {
    let inserted = transaction.execute(
        "INSERT INTO projection_materialization_items (
            projection_id, item_key, owner_key, sort_key, value_json
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(projection_id, item_key) DO NOTHING",
        params![
            projection.as_str(),
            item.item_key,
            item.owner_key,
            item.sort_key,
            serde_json::to_string(&item.value)?,
        ],
    )?;
    if inserted != 1 {
        return Err(PersistenceError::SchemaInvariant(format!(
            "projection item delta insert requires absent item_key {} for {}",
            item.item_key,
            projection.as_str()
        )));
    }
    replace_visible_blob_refs(transaction, projection, item)?;
    Ok(())
}

fn update_projection_item(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    item: &ProjectionItem,
) -> Result<(), PersistenceError> {
    let updated = transaction.execute(
        "UPDATE projection_materialization_items
         SET owner_key = ?3, sort_key = ?4, value_json = ?5
         WHERE projection_id = ?1 AND item_key = ?2",
        params![
            projection.as_str(),
            item.item_key,
            item.owner_key,
            item.sort_key,
            serde_json::to_string(&item.value)?,
        ],
    )?;
    if updated != 1 {
        return Err(PersistenceError::SchemaInvariant(format!(
            "projection item delta update requires existing item_key {} for {}",
            item.item_key,
            projection.as_str()
        )));
    }
    replace_visible_blob_refs(transaction, projection, item)?;
    Ok(())
}

fn delete_projection_item(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    item_key: &str,
) -> Result<(), PersistenceError> {
    let deleted = transaction.execute(
        "DELETE FROM projection_materialization_items
         WHERE projection_id = ?1 AND item_key = ?2",
        params![projection.as_str(), item_key],
    )?;
    if deleted != 1 {
        return Err(PersistenceError::SchemaInvariant(format!(
            "projection item delta delete requires existing item_key {item_key} for {}",
            projection.as_str()
        )));
    }
    transaction.execute(
        "DELETE FROM projection_visible_blob_refs
         WHERE projection_id = ?1 AND item_key = ?2",
        params![projection.as_str(), item_key],
    )?;
    Ok(())
}

fn replace_visible_blob_refs(
    transaction: &rusqlite::Transaction<'_>,
    projection: &lethe_core::domain::ProjectionRef,
    item: &ProjectionItem,
) -> Result<(), PersistenceError> {
    transaction.execute(
        "DELETE FROM projection_visible_blob_refs
         WHERE projection_id = ?1 AND item_key = ?2",
        params![projection.as_str(), item.item_key],
    )?;
    let consent_scope = format!(
        "projection:{}:owner:{}",
        projection.as_str(),
        item.owner_key
    );
    let mut refs = std::collections::BTreeSet::new();
    collect_blob_refs(&item.value, &mut refs);
    for blob_ref in refs {
        transaction.execute(
            "INSERT INTO projection_visible_blob_refs (
                projection_id, item_key, blob_ref, owner_key, consent_scope, subject_key
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                projection.as_str(),
                item.item_key,
                blob_ref,
                item.owner_key,
                consent_scope,
                item.item_key,
            ],
        )?;
    }
    Ok(())
}

fn collect_blob_refs(value: &serde_json::Value, refs: &mut std::collections::BTreeSet<String>) {
    match value {
        serde_json::Value::String(value) if value.starts_with("blob:sha256:") => {
            refs.insert(value.clone());
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_blob_refs(value, refs);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_blob_refs(value, refs);
            }
        }
        _ => {}
    }
}

fn storage_error(error: PersistenceError) -> StorageError {
    match error {
        PersistenceError::SchemaInvariant(message) => StorageError::Invariant(message),
        PersistenceError::CutoverAdmissionDenied(message) => {
            StorageError::CutoverAdmissionDenied(message)
        }
        other => StorageError::Backend(other.to_string()),
    }
}

fn port_outcome(outcome: DurableAppendOutcome) -> PortAppendOutcome {
    match outcome {
        DurableAppendOutcome::Appended(id) => PortAppendOutcome::Appended(id),
        DurableAppendOutcome::Duplicate(id) => PortAppendOutcome::Duplicate(id),
        DurableAppendOutcome::CanonicalCollision(id) => PortAppendOutcome::CanonicalCollision(id),
    }
}

impl ObservationStorePort for SqlitePersistence {
    fn append_observation(&self, observation: &Observation) -> StorageResult<PortAppendOutcome> {
        self.append_observation_idempotent(observation)
            .map(port_outcome)
            .map_err(storage_error)
    }

    fn append_observations(
        &self,
        observations: &[Observation],
    ) -> StorageResult<Vec<PortAppendOutcome>> {
        self.append_observations_idempotent(observations)
            .map(|outcomes| outcomes.into_iter().map(port_outcome).collect())
            .map_err(storage_error)
    }

    fn append_observations_with_audit(
        &self,
        observations: &[Observation],
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> StorageResult<Vec<PortAppendOutcome>> {
        self.append_observations_idempotent_with_audit(observations, audit_events)
            .map(|outcomes| outcomes.into_iter().map(port_outcome).collect())
            .map_err(storage_error)
    }

    fn load_observations(&self) -> StorageResult<Vec<Observation>> {
        SqlitePersistence::load_observations(self).map_err(storage_error)
    }

    fn observation_stats(&self) -> StorageResult<ObservationStats> {
        SqlitePersistence::observation_stats(self).map_err(storage_error)
    }

    fn rehome_observation(
        &self,
        observation: &Observation,
        mode: PortRehomeMode,
    ) -> StorageResult<PortAppendOutcome> {
        let mode = match mode {
            PortRehomeMode::StoredIdentity => RehomeMode::StoredIdentity,
            PortRehomeMode::RecomputedIdentity {
                identity_key,
                canonical_json,
            } => RehomeMode::RecomputedIdentity {
                identity_key,
                canonical_json,
            },
        };
        SqlitePersistence::rehome_observation(self, observation, mode)
            .map(port_outcome)
            .map_err(storage_error)
    }

    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>> {
        SqlitePersistence::observation_page(self, after_append_seq, limit).map_err(storage_error)
    }

    fn observations_for_leaf_after(
        &self,
        leaf_id: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>> {
        SqlitePersistence::observations_for_leaf_after(self, leaf_id, after_append_seq, limit)
            .map_err(storage_error)
    }

    fn observation_by_id(&self, id: &ObservationId) -> StorageResult<Option<StoredObservation>> {
        SqlitePersistence::observation_by_id(self, id).map_err(storage_error)
    }

    fn observations_for_privacy_key(
        &self,
        privacy_key: &str,
    ) -> StorageResult<Vec<StoredObservation>> {
        SqlitePersistence::observations_for_privacy_key(self, privacy_key).map_err(storage_error)
    }

    fn leaf_positions(&self) -> StorageResult<Vec<LeafPosition>> {
        SqlitePersistence::leaf_positions(self).map_err(storage_error)
    }

    fn split_leaf_if_capacity(&self, capacity: usize) -> StorageResult<bool> {
        SqlitePersistence::split_leaf_if_capacity(self, capacity).map_err(storage_error)
    }
}

impl BlobStorePort for SqlitePersistence {
    fn put_blob(&self, data: &[u8], max_bytes: usize) -> StorageResult<BlobRef> {
        if data.len() > max_bytes {
            return Err(StorageError::Invariant(format!(
                "blob size {} exceeds configured maximum {max_bytes}",
                data.len()
            )));
        }
        self.persist_blob(data).map_err(storage_error)
    }

    fn put_blobs(&self, data: &[&[u8]], max_bytes: usize) -> StorageResult<Vec<BlobRef>> {
        if let Some((index, blob)) = data
            .iter()
            .enumerate()
            .find(|(_, blob)| blob.len() > max_bytes)
        {
            return Err(StorageError::Invariant(format!(
                "blob at batch index {index} has size {} exceeding configured maximum {max_bytes}",
                blob.len()
            )));
        }
        self.persist_blobs(data).map_err(storage_error)
    }

    fn get_blob(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>> {
        self.load_blob(blob_ref).map_err(storage_error)
    }
}

impl SupplementalStorePort for SqlitePersistence {
    fn put_supplemental(&self, record: &SupplementalRecord) -> StorageResult<()> {
        self.persist_supplemental(record).map_err(storage_error)
    }

    fn load_supplementals(&self) -> StorageResult<Vec<SupplementalRecord>> {
        SqlitePersistence::load_supplementals(self).map_err(storage_error)
    }

    fn supplemental_by_id(&self, id: &SupplementalId) -> StorageResult<Option<SupplementalRecord>> {
        SqlitePersistence::supplemental_by_id(self, id).map_err(storage_error)
    }

    fn supplemental_page(
        &self,
        after_created_at: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<SupplementalRecord>> {
        SqlitePersistence::supplemental_page(self, after_created_at, limit).map_err(storage_error)
    }
}

impl SupplementalProjectionCommitterPort for SqlitePersistence {
    fn commit_supplemental_and_projection(
        &self,
        record: &SupplementalRecord,
        projection: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
    ) -> StorageResult<()> {
        SqlitePersistence::commit_supplemental_and_projection(
            self, record, projection, manifest, item_delta,
        )
        .map_err(storage_error)
    }

    fn commit_supplemental_and_projection_with_audit(
        &self,
        record: &SupplementalRecord,
        projection: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        item_delta: &ProjectionItemCommit,
        audit_event: &lethe_storage_api::AuditEventRecord,
    ) -> StorageResult<()> {
        SqlitePersistence::commit_supplemental_and_projection_with_audit(
            self,
            record,
            projection,
            manifest,
            item_delta,
            std::slice::from_ref(audit_event),
        )
        .map_err(storage_error)
    }
}

impl ProjectionMaterializerPort for SqlitePersistence {
    fn materialize_projection(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        records: &serde_json::Value,
    ) -> StorageResult<()> {
        SqlitePersistence::materialize_projection(self, projection, records).map_err(storage_error)
    }

    fn projection_records(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
    ) -> StorageResult<Option<serde_json::Value>> {
        SqlitePersistence::projection_records(self, projection).map_err(storage_error)
    }

    fn commit_projection_items(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        commit: &ProjectionItemCommit,
    ) -> StorageResult<()> {
        SqlitePersistence::commit_projection_items(self, projection, manifest, commit)
            .map_err(storage_error)
    }

    fn publish_projection_items_from_staging(
        &self,
        target: &lethe_core::domain::ProjectionRef,
        staging: &lethe_core::domain::ProjectionRef,
        manifest: &serde_json::Value,
        expected_item_count: u64,
    ) -> StorageResult<()> {
        SqlitePersistence::publish_projection_items_from_staging(
            self,
            target,
            staging,
            manifest,
            expected_item_count,
        )
        .map_err(storage_error)
    }

    fn projection_item_by_key(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        item_key: &str,
    ) -> StorageResult<Option<ProjectionItem>> {
        SqlitePersistence::projection_item_by_key(self, projection, item_key).map_err(storage_error)
    }

    fn projection_items_by_owner(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        owner_key: &str,
    ) -> StorageResult<Vec<ProjectionItem>> {
        SqlitePersistence::projection_items_by_owner(self, projection, owner_key)
            .map_err(storage_error)
    }

    fn projection_items_page(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        owner_keys: &[String],
        item_key_prefix: Option<&str>,
        after_sort_key: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<ProjectionItem>> {
        SqlitePersistence::projection_items_page(
            self,
            projection,
            owner_keys,
            item_key_prefix,
            after_sort_key,
            limit,
        )
        .map_err(storage_error)
    }

    fn projection_blob_ref_visible(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        blob_ref: &BlobRef,
    ) -> StorageResult<bool> {
        SqlitePersistence::projection_blob_ref_visible(self, projection, blob_ref)
            .map_err(storage_error)
    }

    fn projection_item_count_by_owner(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        owner_key: &str,
    ) -> StorageResult<u64> {
        SqlitePersistence::projection_item_count_by_owner(self, projection, owner_key)
            .map_err(storage_error)
    }

    fn projection_item_count(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
    ) -> StorageResult<u64> {
        SqlitePersistence::projection_item_count(self, projection).map_err(storage_error)
    }
}

impl RuntimeStateStorePort for SqlitePersistence {
    fn get_state(&self, key: &str) -> StorageResult<Option<String>> {
        SqlitePersistence::get_state(self, key).map_err(storage_error)
    }

    fn set_state(&self, key: &str, value: &str) -> StorageResult<()> {
        SqlitePersistence::set_state(self, key, value).map_err(storage_error)
    }

    fn record_dead_letter(&self, source: &str, reason: &str) -> StorageResult<()> {
        SqlitePersistence::record_dead_letter(self, source, reason).map_err(storage_error)
    }

    fn record_audit_event(
        &self,
        id: &str,
        timestamp: &str,
        actor: &str,
        event_json: &str,
    ) -> StorageResult<()> {
        SqlitePersistence::record_audit_event(self, id, timestamp, actor, event_json)
            .map_err(storage_error)
    }

    fn audit_event_page(
        &self,
        after: Option<&lethe_storage_api::AuditEventCursor>,
        limit: usize,
    ) -> StorageResult<Vec<lethe_storage_api::AuditEventRecord>> {
        SqlitePersistence::audit_event_page(self, after, limit).map_err(storage_error)
    }

    fn record_sync_metrics(&self, source: &str, metrics: &SyncMetricRecord) -> StorageResult<()> {
        SqlitePersistence::record_sync_metrics(self, source, metrics).map_err(storage_error)
    }

    fn record_sync_state(&self, source: &str, state: &PersistedSyncState) -> StorageResult<()> {
        SqlitePersistence::record_sync_state(self, source, state).map_err(storage_error)
    }

    fn load_sync_state(&self, source: &str) -> StorageResult<Option<PersistedSyncState>> {
        SqlitePersistence::load_sync_state(self, source).map_err(storage_error)
    }

    fn apply_retention(&self, retention_days: u32) -> StorageResult<usize> {
        SqlitePersistence::apply_retention(self, retention_days).map_err(storage_error)
    }

    fn garbage_collect_orphan_blobs(&self) -> StorageResult<usize> {
        SqlitePersistence::garbage_collect_orphan_blobs(self).map_err(storage_error)
    }

    fn deep_check(&self) -> StorageResult<()> {
        SqlitePersistence::deep_check(self).map_err(storage_error)
    }
}

impl SlackThreadCatalogStorePort for SqlitePersistence {
    fn append_slack_observation(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
    ) -> StorageResult<PortAppendOutcome> {
        SqlitePersistence::append_slack_observation(self, observation, thread)
            .map(port_outcome)
            .map_err(storage_error)
    }

    fn append_slack_observation_with_audit(
        &self,
        observation: &Observation,
        thread: &SlackThreadKey,
        audit_events: &[lethe_storage_api::AuditEventRecord],
    ) -> StorageResult<PortAppendOutcome> {
        SqlitePersistence::append_slack_observation_with_audit(
            self,
            observation,
            thread,
            audit_events,
        )
        .map(port_outcome)
        .map_err(storage_error)
    }

    fn slack_thread_discovery_high_water(&self) -> StorageResult<u64> {
        SqlitePersistence::slack_thread_discovery_high_water(self).map_err(storage_error)
    }

    fn commit_slack_thread_discovery(
        &self,
        high_water: u64,
        threads: &[DiscoveredSlackThread],
    ) -> StorageResult<()> {
        SqlitePersistence::commit_slack_thread_discovery(self, high_water, threads)
            .map_err(storage_error)
    }

    fn advance_slack_thread_poll_generation(&self) -> StorageResult<u64> {
        SqlitePersistence::advance_slack_thread_poll_generation(self).map_err(storage_error)
    }

    fn slack_threads_to_poll(
        &self,
        source_instance: &str,
        channel_id: &str,
        generation: u64,
        limit: usize,
    ) -> StorageResult<Vec<SlackThreadCatalogEntry>> {
        SqlitePersistence::slack_threads_to_poll(
            self,
            source_instance,
            channel_id,
            generation,
            limit,
        )
        .map_err(storage_error)
    }

    fn complete_slack_thread_poll(
        &self,
        key: &SlackThreadKey,
        generation: u64,
        reply_cursor: &str,
        active: bool,
        next_poll_generation: u64,
    ) -> StorageResult<()> {
        SqlitePersistence::complete_slack_thread_poll(
            self,
            key,
            generation,
            reply_cursor,
            active,
            next_poll_generation,
        )
        .map_err(storage_error)
    }

    fn slack_thread_catalog(
        &self,
        source_instance: &str,
        channel_id: &str,
    ) -> StorageResult<Vec<SlackThreadCatalogEntry>> {
        SqlitePersistence::slack_thread_catalog(self, source_instance, channel_id)
            .map_err(storage_error)
    }
}

impl ProjectionWatermarkStorePort for SqlitePersistence {
    fn projection_leaf_watermark(
        &self,
        projection: &lethe_core::domain::ProjectionRef,
        leaf_id: &str,
    ) -> StorageResult<ProjectionLeafWatermark> {
        SqlitePersistence::projection_leaf_watermark(self, projection, leaf_id)
            .map_err(storage_error)
    }

    fn commit_projection_leaf_watermark(
        &self,
        watermark: &ProjectionLeafWatermark,
    ) -> StorageResult<()> {
        SqlitePersistence::commit_projection_leaf_watermark(self, watermark).map_err(storage_error)
    }
}

fn canonical_json_sha256(canonical_json: &str) -> String {
    hex::encode(sha2::Sha256::digest(canonical_json.as_bytes()))
}

fn canonical_json_from_observation_json(
    observation_json: &str,
) -> Result<String, PersistenceError> {
    let observation: Observation = serde_json::from_str(observation_json)?;
    observation
        .meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            PersistenceError::SchemaInvariant(
                "stored observation.meta.canonical_json is required for duplicate detection"
                    .to_owned(),
            )
        })
}

fn require_identity_and_canonical_json(observation: &Observation) -> Result<(), PersistenceError> {
    if observation
        .meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        return Err(PersistenceError::SchemaInvariant(
            "rehome mode StoredIdentity requires observation.meta.canonical_json".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
