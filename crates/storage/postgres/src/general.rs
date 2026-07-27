use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use lethe_core::domain::{DataSpaceId, Observation, ObservationId, observation_privacy_keys};
use lethe_runtime::runtime::partition::{
    RoutedObservation, RoutingKeyOrder, plan_capacity_split, routing_key_from_observation_for_order,
};
use lethe_storage_api::{
    AppendOutcome, AuditEventRecord, LeafPosition, ObservationStats, ObservationStore, RehomeMode,
    StorageError, StorageResult, StoredObservation,
};
use postgres::{Client, NoTls, Transaction};

use super::general_migrations::{MigrationOutcome, apply_general_migrations};
use super::general_s3::lock_blob_admission;
use super::general_s3::{S3BlobStore, S3BlobStoreConfig};
use super::{quote_identifier, validate_identifier, validate_non_blank};

const CANONICAL_JSON_META_KEY: &str = "canonical_json";
const PARTITION_LOCK_KEY: &str = "lethe:general-observation-partition";

/// Admitted PostgreSQL writer and read pool for the general Observation Lake.
///
/// Construction is all-or-nothing: no client is returned until migrations,
/// role/schema/data-space pins, writer privileges, and every read connection
/// have been verified.
pub struct PostgresPersistence {
    writer: Mutex<Client>,
    readers: Vec<Mutex<Client>>,
    next_reader: AtomicUsize,
    data_space_id: DataSpaceId,
    schema: String,
    role: String,
    migration_outcome: MigrationOutcome,
    blob_store: Option<S3BlobStore>,
}

impl ObservationStore for PostgresPersistence {
    fn append_observation(&self, observation: &Observation) -> StorageResult<AppendOutcome> {
        let mut outcomes = self.append_observations(std::slice::from_ref(observation))?;
        outcomes.pop().ok_or_else(|| {
            StorageError::Invariant("PostgreSQL append returned no outcome".to_owned())
        })
    }

    fn append_observations(
        &self,
        observations: &[Observation],
    ) -> StorageResult<Vec<AppendOutcome>> {
        self.append_observations_with_audit(observations, &[])
    }

    fn append_observations_with_audit(
        &self,
        observations: &[Observation],
        audit_events: &[AuditEventRecord],
    ) -> StorageResult<Vec<AppendOutcome>> {
        validate_append_inputs(observations, audit_events)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_blob_admission(&mut transaction)?;
        self.verify_blob_references_admitted(&observation_blob_refs(observations))?;
        lock_partition_tree(&mut transaction)?;
        let outcomes = append_observations_transaction(&mut transaction, observations)?;
        insert_audit_events(&mut transaction, audit_events)?;
        transaction.commit().map_err(backend)?;
        Ok(outcomes)
    }

    fn load_observations(&self) -> StorageResult<Vec<Observation>> {
        let mut reader = self.reader()?;
        reader
            .query(
                "SELECT observation_json::text
                 FROM observations ORDER BY append_seq",
                &[],
            )
            .map_err(backend)?
            .into_iter()
            .map(|row| deserialize_observation(row.get(0)))
            .collect()
    }

    fn observation_stats(&self) -> StorageResult<ObservationStats> {
        let mut reader = self.reader()?;
        let row = reader
            .query_one(
                "SELECT COUNT(*), COALESCE(MAX(append_seq), 0)
                 FROM observations",
                &[],
            )
            .map_err(backend)?;
        Ok(ObservationStats {
            count: from_i64("observation count", row.get(0))?,
            max_append_seq: from_i64("max append sequence", row.get(1))?,
        })
    }

    fn rehome_observation(
        &self,
        observation: &Observation,
        mode: RehomeMode,
    ) -> StorageResult<AppendOutcome> {
        let mut rehomed = observation.clone();
        match mode {
            RehomeMode::StoredIdentity => {
                required_canonical_json(&rehomed)?;
            }
            RehomeMode::RecomputedIdentity {
                identity_key,
                canonical_json,
            } => {
                if canonical_json.trim().is_empty() {
                    return Err(StorageError::Invariant(
                        "recomputed canonical_json must not be blank".to_owned(),
                    ));
                }
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
        self.append_observation(&rehomed)
    }

    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>> {
        validate_positive_limit("observation page", limit)?;
        let after = to_i64("after_append_seq", after_append_seq)?;
        let limit = usize_to_i64("observation page limit", limit)?;
        let mut reader = self.reader()?;
        stored_observation_rows(
            reader
                .query(
                    "SELECT leaf_id, append_seq, observation_json::text
                     FROM observations
                     WHERE append_seq > $1
                     ORDER BY append_seq
                     LIMIT $2",
                    &[&after, &limit],
                )
                .map_err(backend)?,
        )
    }

    fn observations_for_leaf_after(
        &self,
        leaf_id: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>> {
        require_value("leaf_id", leaf_id)?;
        validate_positive_limit("leaf tail", limit)?;
        let after = to_i64("after_append_seq", after_append_seq)?;
        let limit = usize_to_i64("leaf tail limit", limit)?;
        let mut reader = self.reader()?;
        stored_observation_rows(
            reader
                .query(
                    "SELECT leaf_id, append_seq, observation_json::text
                     FROM observations
                     WHERE leaf_id = $1 AND append_seq > $2
                     ORDER BY append_seq
                     LIMIT $3",
                    &[&leaf_id, &after, &limit],
                )
                .map_err(backend)?,
        )
    }

    fn observation_by_id(&self, id: &ObservationId) -> StorageResult<Option<StoredObservation>> {
        let mut reader = self.reader()?;
        reader
            .query_opt(
                "SELECT leaf_id, append_seq, observation_json::text
                 FROM observations WHERE observation_id = $1",
                &[&id.as_str()],
            )
            .map_err(backend)?
            .map(stored_observation_row)
            .transpose()
    }

    fn observations_for_privacy_key(
        &self,
        privacy_key: &str,
    ) -> StorageResult<Vec<StoredObservation>> {
        require_value("privacy key", privacy_key)?;
        let mut reader = self.reader()?;
        stored_observation_rows(
            reader
                .query(
                    "SELECT observations.leaf_id, observations.append_seq,
                            observations.observation_json::text
                     FROM observation_privacy_keys reverse_index
                     JOIN observations
                       ON observations.append_seq = reverse_index.append_seq
                     WHERE reverse_index.privacy_key = $1
                     ORDER BY observations.append_seq",
                    &[&privacy_key],
                )
                .map_err(backend)?,
        )
    }

    fn observations_for_privacy_key_page(
        &self,
        privacy_key: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>> {
        require_value("privacy key", privacy_key)?;
        validate_positive_limit("privacy key page", limit)?;
        let after = to_i64("after_append_seq", after_append_seq)?;
        let limit = usize_to_i64("privacy key page limit", limit)?;
        let mut reader = self.reader()?;
        stored_observation_rows(
            reader
                .query(
                    "SELECT observations.leaf_id, observations.append_seq,
                            observations.observation_json::text
                     FROM observation_privacy_keys reverse_index
                     JOIN observations
                       ON observations.append_seq = reverse_index.append_seq
                     WHERE reverse_index.privacy_key = $1
                       AND observations.append_seq > $2
                     ORDER BY observations.append_seq
                     LIMIT $3",
                    &[&privacy_key, &after, &limit],
                )
                .map_err(backend)?,
        )
    }

    fn leaf_positions(&self) -> StorageResult<Vec<LeafPosition>> {
        let mut reader = self.reader()?;
        reader
            .query(
                "SELECT leaves.leaf_id, COALESCE(MAX(observations.append_seq), 0)
                 FROM observation_leaves leaves
                 LEFT JOIN observations
                   ON observations.leaf_id = leaves.leaf_id
                 WHERE leaves.active
                 GROUP BY leaves.leaf_id
                 ORDER BY leaves.leaf_id",
                &[],
            )
            .map_err(backend)?
            .into_iter()
            .map(|row| {
                Ok(LeafPosition {
                    leaf_id: row.get(0),
                    append_seq: from_i64("leaf append sequence", row.get(1))?,
                })
            })
            .collect()
    }

    fn split_leaf_if_capacity(&self, capacity: usize) -> StorageResult<bool> {
        validate_positive_limit("leaf capacity", capacity)?;
        let mut writer = self.writer()?;
        let mut transaction = writer.transaction().map_err(backend)?;
        lock_partition_tree(&mut transaction)?;
        let active_leaves = transaction
            .query(
                "SELECT leaf_id FROM observation_leaves
                 WHERE active ORDER BY leaf_id FOR UPDATE",
                &[],
            )
            .map_err(backend)?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>();
        for parent_leaf_id in active_leaves {
            if split_leaf(&mut transaction, &parent_leaf_id, capacity)? {
                transaction.commit().map_err(backend)?;
                return Ok(true);
            }
        }
        transaction.commit().map_err(backend)?;
        Ok(false)
    }
}

pub(super) fn observation_blob_refs(
    observations: &[Observation],
) -> Vec<lethe_core::domain::BlobRef> {
    observations
        .iter()
        .flat_map(|observation| observation.attachments.iter().cloned())
        .collect()
}

pub(super) fn validate_append_inputs(
    observations: &[Observation],
    audit_events: &[AuditEventRecord],
) -> StorageResult<()> {
    for observation in observations {
        require_value("observation id", observation.id.as_str())?;
        require_value(
            "observation idempotency key",
            observation.idempotency_key.as_str(),
        )?;
        required_canonical_json(observation)?;
        routing_key_from_observation_for_order(
            RoutingKeyOrder::MonthYearSourceContainerPublished,
            observation,
        )
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
    }
    for audit in audit_events {
        super::general_runtime::validate_audit_record(audit)?;
    }
    Ok(())
}

pub(super) fn append_observations_transaction(
    transaction: &mut Transaction<'_>,
    observations: &[Observation],
) -> StorageResult<Vec<AppendOutcome>> {
    let mut outcomes = Vec::with_capacity(observations.len());
    for observation in observations {
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&format!(
                    "lethe:observation-identity:{}",
                    observation.idempotency_key.as_str()
                )],
            )
            .map_err(backend)?;
        if let Some(outcome) = existing_observation_outcome(transaction, observation)? {
            outcomes.push(outcome);
            continue;
        }
        reject_observation_id_collision(transaction, observation)?;
        let routing_key = routing_key_from_observation_for_order(
            RoutingKeyOrder::MonthYearSourceContainerPublished,
            observation,
        )
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
        let routing_key = routing_key.encoded();
        let leaf_id = route_leaf(transaction, routing_key.as_bytes())?;
        let observation_json = serde_json::to_string(observation)
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let canonical_json = required_canonical_json(observation)?;
        let previous_append_seq: i64 = transaction
            .query_one("SELECT COALESCE(MAX(append_seq), 0) FROM observations", &[])
            .map_err(backend)?
            .get(0);
        let append_seq = previous_append_seq.checked_add(1).ok_or_else(|| {
            StorageError::Invariant("observation append sequence exhausted BIGINT".to_owned())
        })?;
        transaction
            .query_one(
                "INSERT INTO observations (
                    append_seq, observation_id, identity_key, canonical_json, routing_key,
                    leaf_id, observed_at, observation_json
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb)
                 RETURNING append_seq",
                &[
                    &append_seq,
                    &observation.id.as_str(),
                    &observation.idempotency_key.as_str(),
                    &canonical_json,
                    &routing_key,
                    &leaf_id,
                    &observation.recorded_at.to_rfc3339(),
                    &observation_json,
                ],
            )
            .map_err(backend)?;
        transaction
            .execute(
                "UPDATE observation_leaves
                 SET observation_count = observation_count + 1
                 WHERE leaf_id = $1 AND active",
                &[&leaf_id],
            )
            .map_err(backend)?;
        for privacy_key in observation_privacy_keys(observation) {
            transaction
                .execute(
                    "INSERT INTO observation_privacy_keys (
                        privacy_key, append_seq
                     ) VALUES ($1, $2)",
                    &[&privacy_key, &append_seq],
                )
                .map_err(backend)?;
        }
        outcomes.push(AppendOutcome::Appended(observation.id.clone()));
    }
    Ok(outcomes)
}

fn existing_observation_outcome(
    transaction: &mut Transaction<'_>,
    observation: &Observation,
) -> StorageResult<Option<AppendOutcome>> {
    let existing = transaction
        .query_opt(
            "SELECT observation_id, canonical_json
             FROM observations WHERE identity_key = $1",
            &[&observation.idempotency_key.as_str()],
        )
        .map_err(backend)?;
    let Some(row) = existing else {
        return Ok(None);
    };
    let existing_id = ObservationId::new(row.get::<_, String>(0));
    let existing_canonical: String = row.get(1);
    Ok(Some(
        if existing_canonical == required_canonical_json(observation)? {
            AppendOutcome::Duplicate(existing_id)
        } else {
            AppendOutcome::CanonicalCollision(existing_id)
        },
    ))
}

fn reject_observation_id_collision(
    transaction: &mut Transaction<'_>,
    observation: &Observation,
) -> StorageResult<()> {
    let existing_identity = transaction
        .query_opt(
            "SELECT identity_key FROM observations WHERE observation_id = $1",
            &[&observation.id.as_str()],
        )
        .map_err(backend)?
        .map(|row| row.get::<_, String>(0));
    if let Some(identity) = existing_identity {
        return Err(StorageError::Invariant(format!(
            "observation id {} already belongs to identity key {identity}",
            observation.id
        )));
    }
    Ok(())
}

pub(super) fn insert_audit_events(
    transaction: &mut Transaction<'_>,
    audit_events: &[AuditEventRecord],
) -> StorageResult<()> {
    for audit in audit_events {
        super::general_runtime::insert_audit_record(transaction, audit)?;
    }
    Ok(())
}

pub(super) fn lock_partition_tree(transaction: &mut Transaction<'_>) -> StorageResult<()> {
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&PARTITION_LOCK_KEY],
        )
        .map_err(backend)?;
    Ok(())
}

fn route_leaf(transaction: &mut Transaction<'_>, routing_key: &[u8]) -> StorageResult<String> {
    let mut leaf_id: String = transaction
        .query_one(
            "SELECT leaf_id FROM observation_leaves
             WHERE parent_leaf_id IS NULL",
            &[],
        )
        .map_err(backend)?
        .get(0);
    loop {
        let row = transaction
            .query_one(
                "SELECT active, split_bit_index
                 FROM observation_leaves WHERE leaf_id = $1",
                &[&leaf_id],
            )
            .map_err(backend)?;
        let active: bool = row.get(0);
        let split_bit_index: Option<i32> = row.get(1);
        if active {
            return Ok(leaf_id);
        }
        let bit_index = split_bit_index.ok_or_else(|| {
            StorageError::Invariant(format!("retired leaf {leaf_id} has no split_bit_index"))
        })?;
        let bit_index = usize::try_from(bit_index).map_err(|_| {
            StorageError::Invariant(format!("leaf {leaf_id} has negative split bit"))
        })?;
        let child_side: i16 = i16::from(bit_at(routing_key, bit_index));
        leaf_id = transaction
            .query_one(
                "SELECT leaf_id FROM observation_leaves
                 WHERE parent_leaf_id = $1 AND child_side = $2",
                &[&leaf_id, &child_side],
            )
            .map_err(backend)?
            .get(0);
    }
}

fn split_leaf(
    transaction: &mut Transaction<'_>,
    parent_leaf_id: &str,
    capacity: usize,
) -> StorageResult<bool> {
    let rows = transaction
        .query(
            "SELECT observation_id, observation_json::text
             FROM observations
             WHERE leaf_id = $1
             ORDER BY append_seq",
            &[&parent_leaf_id],
        )
        .map_err(backend)?;
    let mut routed = Vec::with_capacity(rows.len());
    for row in rows {
        let observation_id: String = row.get(0);
        let observation = deserialize_observation(row.get(1))?;
        let routing_key = routing_key_from_observation_for_order(
            RoutingKeyOrder::MonthYearSourceContainerPublished,
            &observation,
        )
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
        routed.push(RoutedObservation {
            observation_id,
            routing_key,
        });
    }
    let left = format!("lake:{}", uuid::Uuid::now_v7());
    let right = format!("lake:{}", uuid::Uuid::now_v7());
    let Some(plan) = plan_capacity_split(parent_leaf_id, &routed, capacity, &left, &right)
        .map_err(|error| StorageError::Invariant(error.to_string()))?
    else {
        return Ok(false);
    };
    let bit_index = i32::try_from(plan.bit_index).map_err(|_| {
        StorageError::Invariant("partition split bit exceeds PostgreSQL INTEGER".to_owned())
    })?;
    transaction
        .execute(
            "UPDATE observation_leaves
             SET active = FALSE, split_bit_index = $2, observation_count = 0
             WHERE leaf_id = $1 AND active",
            &[&parent_leaf_id, &bit_index],
        )
        .map_err(backend)?;
    for (leaf_id, side) in [(&left, 0_i16), (&right, 1_i16)] {
        transaction
            .execute(
                "INSERT INTO observation_leaves (
                    leaf_id, parent_leaf_id, child_side
                 ) VALUES ($1, $2, $3)",
                &[&leaf_id, &parent_leaf_id, &side],
            )
            .map_err(backend)?;
    }
    for target in &plan.rehome_targets {
        transaction
            .execute(
                "UPDATE observations SET leaf_id = $1
                 WHERE observation_id = $2 AND leaf_id = $3",
                &[
                    &target.target_leaf_id,
                    &target.observation_id,
                    &parent_leaf_id,
                ],
            )
            .map_err(backend)?;
    }
    transaction
        .execute(
            "UPDATE observation_leaves leaves
             SET observation_count = (
                SELECT COUNT(*) FROM observations
                WHERE observations.leaf_id = leaves.leaf_id
             )
             WHERE leaves.leaf_id IN ($1, $2)",
            &[&left, &right],
        )
        .map_err(backend)?;
    Ok(true)
}

fn bit_at(bytes: &[u8], bit_index: usize) -> bool {
    let byte_index = bit_index / 8;
    let offset = 7 - (bit_index % 8);
    bytes
        .get(byte_index)
        .is_some_and(|byte| (byte & (1 << offset)) != 0)
}

fn stored_observation_rows(rows: Vec<postgres::Row>) -> StorageResult<Vec<StoredObservation>> {
    rows.into_iter().map(stored_observation_row).collect()
}

fn stored_observation_row(row: postgres::Row) -> StorageResult<StoredObservation> {
    Ok(StoredObservation {
        leaf_id: row.get(0),
        append_seq: from_i64("append sequence", row.get(1))?,
        observation: deserialize_observation(row.get(2))?,
    })
}

fn deserialize_observation(json: String) -> StorageResult<Observation> {
    serde_json::from_str(&json).map_err(|error| StorageError::Backend(error.to_string()))
}

fn required_canonical_json(observation: &Observation) -> StorageResult<&str> {
    observation
        .meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            StorageError::Invariant(
                "observation.meta.canonical_json is required for durable ingest".to_owned(),
            )
        })
}

fn require_value(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!(
            "{field} must not be blank"
        )))
    } else {
        Ok(())
    }
}

fn validate_positive_limit(field: &str, value: usize) -> StorageResult<()> {
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

fn from_i64(field: &str, value: i64) -> StorageResult<u64> {
    u64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} must not be negative")))
}

impl PostgresPersistence {
    /// Connect a general-Lake pool without transport TLS.
    ///
    /// This entrypoint is intended for an explicitly isolated PostgreSQL
    /// network. It never changes the selected DSN or falls back to SQLite.
    ///
    /// # Errors
    ///
    /// Returns an invariant error for invalid configuration, pin mismatch, or
    /// insufficient writer admission. Connection failures are backend errors.
    pub fn connect_no_tls(
        data_space_id: DataSpaceId,
        dsn: &str,
        schema: &str,
        expected_role: &str,
        read_pool_size: usize,
        blob_config: S3BlobStoreConfig,
    ) -> StorageResult<Self> {
        let blob_store = S3BlobStore::connect(blob_config)?;
        let mut store = Self::connect_database_only_for_tests(
            data_space_id,
            dsn,
            schema,
            expected_role,
            read_pool_size,
        )?;
        store.blob_store = Some(blob_store);
        Ok(store)
    }

    /// Connect only the PostgreSQL half for isolated adapter conformance.
    ///
    /// This constructor deliberately leaves all BlobStore effects
    /// unadmitted. Production assembly must use [`Self::connect_no_tls`].
    ///
    /// # Errors
    ///
    /// Returns the same PostgreSQL admission errors as [`Self::connect_no_tls`].
    pub fn connect_database_only_for_tests(
        data_space_id: DataSpaceId,
        dsn: &str,
        schema: &str,
        expected_role: &str,
        read_pool_size: usize,
    ) -> StorageResult<Self> {
        validate_connection_config(&data_space_id, dsn, schema, expected_role, read_pool_size)?;
        let mut writer = Client::connect(dsn, NoTls).map_err(backend)?;
        let migration_outcome =
            apply_general_migrations(&mut writer, schema, expected_role, &data_space_id)?;
        admit_connection(&mut writer, schema, expected_role, &data_space_id, true)?;

        let mut readers = Vec::with_capacity(read_pool_size);
        for _ in 0..read_pool_size {
            let mut reader = Client::connect(dsn, NoTls).map_err(backend)?;
            admit_connection(&mut reader, schema, expected_role, &data_space_id, false)?;
            readers.push(Mutex::new(reader));
        }
        Ok(Self {
            writer: Mutex::new(writer),
            readers,
            next_reader: AtomicUsize::new(0),
            data_space_id,
            schema: schema.to_owned(),
            role: expected_role.to_owned(),
            migration_outcome,
            blob_store: None,
        })
    }

    /// Pinned data-space identifier.
    pub fn data_space_id(&self) -> &DataSpaceId {
        &self.data_space_id
    }

    /// Pinned schema.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Pinned database role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Number of admitted read connections.
    pub fn read_pool_size(&self) -> usize {
        self.readers.len()
    }

    /// Versions applied while this pool was constructed.
    pub fn migration_outcome(&self) -> &MigrationOutcome {
        &self.migration_outcome
    }

    /// Verify writer and every reader against the persisted pins.
    ///
    /// # Errors
    ///
    /// Returns an error when a mutex is poisoned or any connection/pin check
    /// fails.
    pub fn deep_check_connections(&self) -> StorageResult<()> {
        {
            let mut writer = self.writer()?;
            admit_connection(
                &mut writer,
                &self.schema,
                &self.role,
                &self.data_space_id,
                true,
            )?;
        }
        for reader in &self.readers {
            let mut reader = reader.lock().map_err(|_| {
                StorageError::Backend("postgres reader mutex is poisoned".to_owned())
            })?;
            admit_connection(
                &mut reader,
                &self.schema,
                &self.role,
                &self.data_space_id,
                false,
            )?;
        }
        Ok(())
    }

    /// Execute one round-robin read-pool probe.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected reader cannot execute a query.
    pub fn read_check(&self) -> StorageResult<()> {
        let mut reader = self.reader()?;
        let value: bool = reader
            .query_one("SELECT TRUE", &[])
            .map_err(backend)?
            .get(0);
        if value {
            Ok(())
        } else {
            Err(StorageError::Invariant(
                "PostgreSQL read probe returned false".to_owned(),
            ))
        }
    }

    pub(crate) fn writer(&self) -> StorageResult<MutexGuard<'_, Client>> {
        self.writer
            .lock()
            .map_err(|_| StorageError::Backend("postgres writer mutex is poisoned".to_owned()))
    }

    pub(crate) fn reader(&self) -> StorageResult<MutexGuard<'_, Client>> {
        let index = self.next_reader.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        self.readers[index]
            .lock()
            .map_err(|_| StorageError::Backend("postgres reader mutex is poisoned".to_owned()))
    }

    pub(crate) fn admitted_blob_store(&self) -> StorageResult<&S3BlobStore> {
        self.blob_store.as_ref().ok_or_else(|| {
            StorageError::Invariant(
                "PostgreSQL persistence was opened without an admitted S3 BlobStore".to_owned(),
            )
        })
    }
}

fn validate_connection_config(
    data_space_id: &DataSpaceId,
    dsn: &str,
    schema: &str,
    expected_role: &str,
    read_pool_size: usize,
) -> StorageResult<()> {
    validate_non_blank("data_space_id", data_space_id.as_str())
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
    validate_non_blank("dsn", dsn).map_err(|error| StorageError::Invariant(error.to_string()))?;
    validate_identifier("schema", schema)
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
    validate_identifier("expected_role", expected_role)
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
    if read_pool_size == 0 {
        return Err(StorageError::Invariant(
            "read_pool_size must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn admit_connection(
    client: &mut Client,
    schema: &str,
    expected_role: &str,
    data_space_id: &DataSpaceId,
    require_writer: bool,
) -> StorageResult<()> {
    let current_role: String = client
        .query_one("SELECT current_user", &[])
        .map_err(backend)?
        .get(0);
    if current_role != expected_role {
        return Err(StorageError::Invariant(format!(
            "connected role {current_role} does not match required role {expected_role}"
        )));
    }
    let schema_exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM pg_namespace WHERE nspname = $1
             )",
            &[&schema],
        )
        .map_err(backend)?
        .get(0);
    if !schema_exists {
        return Err(StorageError::Invariant(format!(
            "required schema {schema} does not exist"
        )));
    }
    client
        .batch_execute(&format!(
            "SET search_path TO {}, pg_catalog",
            quote_identifier(schema)
        ))
        .map_err(backend)?;
    if require_writer {
        admit_writer(client, schema)?;
    }
    let row = client
        .query_one(
            "SELECT data_space_id, database_role
             FROM general_storage_pin WHERE singleton",
            &[],
        )
        .map_err(backend)?;
    let pinned_data_space: String = row.get(0);
    let pinned_role: String = row.get(1);
    if pinned_data_space != data_space_id.as_str() || pinned_role != expected_role {
        return Err(StorageError::Invariant(format!(
            "general storage pin mismatch: data_space={pinned_data_space:?}, role={pinned_role:?}"
        )));
    }
    Ok(())
}

fn admit_writer(client: &mut Client, schema: &str) -> StorageResult<()> {
    let transaction_read_only: String = client
        .query_one("SHOW transaction_read_only", &[])
        .map_err(backend)?
        .get(0);
    if transaction_read_only != "off" {
        return Err(StorageError::Invariant(
            "selected PostgreSQL writer is transaction_read_only".to_owned(),
        ));
    }
    let privileges: bool = client
        .query_one(
            "SELECT has_schema_privilege(current_user, $1, 'USAGE')
                    AND has_schema_privilege(current_user, $1, 'CREATE')",
            &[&schema],
        )
        .map_err(backend)?
        .get(0);
    if !privileges {
        return Err(StorageError::Invariant(format!(
            "selected PostgreSQL writer lacks USAGE and CREATE on schema {schema}"
        )));
    }
    Ok(())
}

fn backend(error: postgres::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_read_pool_is_rejected_before_io() {
        let result = PostgresPersistence::connect_database_only_for_tests(
            DataSpaceId::new("space:test"),
            "postgres://unreachable.invalid/test",
            "lethe",
            "lethe_app",
            0,
        );
        assert!(matches!(
            result,
            Err(StorageError::Invariant(reason)) if reason.contains("read_pool_size")
        ));
    }
}
