mod schema;

use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};
use sha2::Digest;

use lethe_core::domain::{BlobRef, IdempotencyKey, Observation, ObservationId, SupplementalRecord};
use lethe_runtime::runtime::partition::{
    PARTITION_EVENT_FAILOVER, PARTITION_EVENT_INITIALIZE, PARTITION_EVENT_RECOVER,
    PARTITION_EVENT_SPLIT_COMMIT, PARTITION_EVENT_SPLIT_PREPARE, PARTITION_SPLIT_REASON_CAPACITY,
    PartitionTree, failover_event_json, identity_keyspec_json, initialize_event_json,
    parse_partition_event, recover_event_json, routing_keyspec_json, split_commit_event_json,
    split_prepare_event_json,
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
}

pub struct SqlitePersistence {
    conn: Connection,
    blob_dir: PathBuf,
}

const CURRENT_SCHEMA_VERSION: i64 = 2;
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

impl SqlitePersistence {
    pub fn open(database_path: &Path, blob_dir: &Path) -> Result<Self, PersistenceError> {
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(blob_dir)?;

        let conn = Connection::open(database_path)?;
        let store = Self {
            conn,
            blob_dir: blob_dir.to_path_buf(),
        };
        store.init_schema()?;
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
        let json = serde_json::to_string(observation)?;
        let inserted = self.conn.execute(
            "INSERT INTO observations (
                id,
                identity_key,
                canonical_json,
                recorded_at,
                observation_json
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                observation.id.as_str(),
                identity_key.as_str(),
                canonical_json,
                observation.recorded_at.to_rfc3339(),
                json,
            ],
        );

        match inserted {
            Ok(_) => Ok(DurableAppendOutcome::Appended(observation.id.clone())),
            Err(insert_err) => {
                let existing = self
                    .conn
                    .query_row(
                        "SELECT id, canonical_json FROM observations WHERE identity_key = ?1",
                        [identity_key.as_str()],
                        |row| {
                            Ok((
                                ObservationId::new(row.get::<_, String>(0)?),
                                row.get::<_, String>(1)?,
                            ))
                        },
                    )
                    .optional()?;

                if let Some((existing_id, existing_canonical_json)) = existing {
                    if existing_canonical_json == canonical_json {
                        Ok(DurableAppendOutcome::Duplicate(existing_id))
                    } else {
                        Ok(DurableAppendOutcome::CanonicalCollision(existing_id))
                    }
                } else {
                    Err(PersistenceError::Sqlite(insert_err))
                }
            }
        }
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

    pub fn persist_blob(&self, data: &[u8]) -> Result<BlobRef, PersistenceError> {
        let hash = hex::encode(sha2::Sha256::digest(data));
        let blob_ref = BlobRef::new(format!("blob:sha256:{hash}"));
        let path = self.blob_dir.join(&hash);
        if !path.exists() {
            fs::write(&path, data)?;
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO blobs (blob_ref, file_path) VALUES (?1, ?2)",
            params![blob_ref.as_str(), path.to_string_lossy().to_string()],
        )?;
        Ok(blob_ref)
    }

    pub fn load_blobs(&self) -> Result<Vec<Vec<u8>>, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare("SELECT file_path FROM blobs ORDER BY file_path")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut blobs = Vec::new();
        for row in rows {
            blobs.push(fs::read(row?)?);
        }
        Ok(blobs)
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
        self.conn.execute(
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
        Ok(self.conn.last_insert_rowid())
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

    pub fn load_partition_tree(&self) -> Result<PartitionTree, PersistenceError> {
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
