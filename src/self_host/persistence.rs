use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};
use sha2::Digest;

use crate::domain::{BlobRef, IdempotencyKey, Observation, SupplementalRecord};
use crate::runtime::partition::{
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
    Appended(crate::domain::ObservationId),
    Duplicate(crate::domain::ObservationId),
    CanonicalCollision(crate::domain::ObservationId),
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
                                crate::domain::ObservationId::new(row.get::<_, String>(0)?),
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

    fn init_schema(&self) -> Result<(), PersistenceError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS observations (
                append_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                identity_key TEXT NOT NULL UNIQUE,
                canonical_json TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                observation_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS blobs (
                blob_ref TEXT PRIMARY KEY,
                file_path TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS supplementals (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                supplemental_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS partition_log (
                event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL CHECK (
                    event_type IN (
                        'initialize',
                        'split_prepare',
                        'split_commit',
                        'failover',
                        'recover'
                    )
                ),
                leaf_id TEXT CHECK (leaf_id IS NULL OR leaf_id LIKE 'lake:%'),
                parent_leaf_id TEXT CHECK (parent_leaf_id IS NULL OR parent_leaf_id LIKE 'lake:%'),
                left_child_leaf_id TEXT CHECK (left_child_leaf_id IS NULL OR left_child_leaf_id LIKE 'lake:%'),
                right_child_leaf_id TEXT CHECK (right_child_leaf_id IS NULL OR right_child_leaf_id LIKE 'lake:%'),
                bit_index INTEGER,
                reason TEXT,
                routing_keyspec_json TEXT,
                identity_keyspec_json TEXT,
                control_timestamp TEXT,
                event_json TEXT NOT NULL,
                CHECK (
                    event_type != 'initialize'
                    OR (
                        leaf_id IS NOT NULL
                        AND routing_keyspec_json IS NOT NULL
                        AND identity_keyspec_json IS NOT NULL
                    )
                ),
                CHECK (
                    event_type != 'split_commit'
                    OR (
                        parent_leaf_id IS NOT NULL
                        AND left_child_leaf_id IS NOT NULL
                        AND right_child_leaf_id IS NOT NULL
                        AND bit_index IS NOT NULL
                        AND reason = 'capacity'
                    )
                )
            );

            CREATE UNIQUE INDEX IF NOT EXISTS partition_log_single_initialize
                ON partition_log(event_type)
                WHERE event_type = 'initialize';

            CREATE TRIGGER IF NOT EXISTS partition_log_no_update
            BEFORE UPDATE ON partition_log
            BEGIN
                SELECT RAISE(ABORT, 'partition_log is append-only');
            END;

            CREATE TRIGGER IF NOT EXISTS partition_log_no_delete
            BEFORE DELETE ON partition_log
            BEGIN
                SELECT RAISE(ABORT, 'partition_log is append-only');
            END;
            ",
        )?;
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
            params![
                CURRENT_SCHEMA_VERSION,
                "partition_log_and_append_seq_observations",
                chrono::Utc::now().to_rfc3339(),
            ],
        )?;
        self.ensure_partition_initialize()?;
        Ok(())
    }

    fn ensure_partition_initialize(&self) -> Result<(), PersistenceError> {
        let expected_routing = routing_keyspec_json()?;
        let expected_identity = identity_keyspec_json()?;

        let existing = self
            .conn
            .query_row(
                "SELECT routing_keyspec_json, identity_keyspec_json
                 FROM partition_log
                 WHERE event_type = ?1",
                [PARTITION_EVENT_INITIALIZE],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;

        if let Some((routing, identity)) = existing {
            if routing != expected_routing || identity != expected_identity {
                return Err(PersistenceError::SchemaInvariant(
                    "partition initialize keyspec does not match compiled keyspec; use blue/green migration"
                        .to_owned(),
                ));
            }
            return Ok(());
        }

        let root_leaf_id = format!("lake:{}", uuid::Uuid::now_v7());
        let event_json = initialize_event_json(&root_leaf_id)?;
        self.conn.execute(
            "INSERT INTO partition_log (
                event_type,
                leaf_id,
                routing_keyspec_json,
                identity_keyspec_json,
                control_timestamp,
                event_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                PARTITION_EVENT_INITIALIZE,
                root_leaf_id,
                expected_routing,
                expected_identity,
                chrono::Utc::now().to_rfc3339(),
                event_json,
            ],
        )?;
        Ok(())
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
mod tests {
    use super::*;
    use chrono::Utc;

    use crate::domain::{
        ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
        ObserverRef, SchemaRef, SemVer, SupplementalId, SupplementalRecord,
        supplemental::InputAnchorSet,
    };

    fn sample_observation() -> Observation {
        let canonical_json = serde_json::json!({
            "source": "test",
            "object_id": "sample-key",
            "body": "world"
        })
        .to_string();
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:test"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: None,
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("entity:test"),
            target: None,
            payload: serde_json::json!({"hello": "world"}),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("sample-key"),
            meta: serde_json::json!({
                CANONICAL_JSON_META_KEY: canonical_json,
            }),
        }
    }

    #[test]
    fn persist_and_reload_observation() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let observation = sample_observation();

        store.persist_observation(&observation).unwrap();
        let observations = store.load_observations().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].schema, observation.schema);

        let _ = fs::remove_dir_all(tmp);
    }

    fn sample_supplemental(observation_id: &crate::domain::ObservationId) -> SupplementalRecord {
        SupplementalRecord {
            id: SupplementalId::new("sup:test"),
            kind: "slide-analysis".into(),
            derived_from: InputAnchorSet {
                observations: vec![observation_id.clone()],
                blobs: vec![],
                supplementals: vec![],
            },
            payload: serde_json::json!({"bio_text": "hello"}),
            created_by: ActorRef::new("actor:test"),
            created_at: Utc::now(),
            mutability: Mutability::ManagedCache,
            record_version: Some("1".into()),
            model_version: Some("fixture".into()),
            consent_metadata: None,
            lineage: None,
        }
    }

    #[test]
    fn duplicate_persist_observation_surfaces_constraint_error() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let observation = sample_observation();

        store.persist_observation(&observation).unwrap();
        let err = store.persist_observation(&observation).unwrap_err();
        assert!(matches!(err, PersistenceError::SchemaInvariant(_)));

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn idempotent_append_returns_duplicate_for_same_canonical_json() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let observation = sample_observation();

        let first = store.append_observation_idempotent(&observation).unwrap();
        let second = store.append_observation_idempotent(&observation).unwrap();

        assert_eq!(
            first,
            DurableAppendOutcome::Appended(observation.id.clone())
        );
        assert_eq!(
            second,
            DurableAppendOutcome::Duplicate(observation.id.clone())
        );

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn idempotent_append_detects_canonical_json_collision() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let observation = sample_observation();
        let mut collision = observation.clone();
        collision.id = Observation::new_id();
        collision.meta = serde_json::json!({
            CANONICAL_JSON_META_KEY: serde_json::json!({
                "source": "test",
                "object_id": "sample-key",
                "body": "changed"
            }).to_string(),
        });

        store.append_observation_idempotent(&observation).unwrap();
        let outcome = store.append_observation_idempotent(&collision).unwrap();

        assert_eq!(
            outcome,
            DurableAppendOutcome::CanonicalCollision(observation.id.clone())
        );

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn rehome_mode_a_preserves_stored_identity_and_times() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let mut first = sample_observation();
        first.idempotency_key = IdempotencyKey::new("first");
        first.meta = serde_json::json!({
            CANONICAL_JSON_META_KEY: serde_json::json!({
                "source": "test",
                "object_id": "first",
                "body": "first"
            }).to_string(),
        });
        store.persist_observation(&first).unwrap();

        let mut observation = sample_observation();
        observation.id = Observation::new_id();
        observation.idempotency_key = IdempotencyKey::new("rehome-mode-a");
        observation.published = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:30:00Z")
            .unwrap()
            .to_utc();
        observation.recorded_at = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:31:00Z")
            .unwrap()
            .to_utc();
        observation.meta = serde_json::json!({
            CANONICAL_JSON_META_KEY: serde_json::json!({
                "source": "test",
                "object_id": "rehome-mode-a",
                "body": "stored"
            }).to_string(),
        });

        let outcome = store
            .rehome_observation(&observation, RehomeMode::StoredIdentity)
            .unwrap();

        assert_eq!(
            outcome,
            DurableAppendOutcome::Appended(observation.id.clone())
        );
        let (append_seq, json): (i64, String) = store
            .conn
            .query_row(
                "SELECT append_seq, observation_json FROM observations WHERE id = ?1",
                [observation.id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let stored = serde_json::from_str::<Observation>(&json).unwrap();

        assert_eq!(append_seq, 2);
        assert_eq!(stored.id, observation.id);
        assert_eq!(stored.published, observation.published);
        assert_eq!(stored.recorded_at, observation.recorded_at);
        assert_eq!(stored.idempotency_key, observation.idempotency_key);

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn rehome_mode_b_reserializes_identity_and_canonical_json() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let observation = sample_observation();
        let new_key = IdempotencyKey::new("identity-v2");
        let new_canonical_json = serde_json::json!({
            "source": "test-v2",
            "object_id": "sample-key",
            "body": "world"
        })
        .to_string();

        let outcome = store
            .rehome_observation(
                &observation,
                RehomeMode::RecomputedIdentity {
                    identity_key: new_key.clone(),
                    canonical_json: new_canonical_json.clone(),
                },
            )
            .unwrap();

        assert_eq!(
            outcome,
            DurableAppendOutcome::Appended(observation.id.clone())
        );
        let (identity_key, canonical_json, json): (String, String, String) = store
            .conn
            .query_row(
                "SELECT identity_key, canonical_json, observation_json FROM observations WHERE id = ?1",
                [observation.id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let stored = serde_json::from_str::<Observation>(&json).unwrap();

        assert_eq!(identity_key, new_key.as_str());
        assert_eq!(canonical_json, new_canonical_json);
        assert_eq!(stored.idempotency_key, new_key);
        assert_eq!(
            stored.meta[CANONICAL_JSON_META_KEY].as_str(),
            Some(new_canonical_json.as_str())
        );
        assert_eq!(stored.id, observation.id);
        assert_eq!(stored.published, observation.published);
        assert_eq!(stored.recorded_at, observation.recorded_at);

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn persist_and_reload_supplemental() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let observation = sample_observation();
        let supplemental = sample_supplemental(&observation.id);

        store.persist_observation(&observation).unwrap();
        store.persist_supplemental(&supplemental).unwrap();
        let supplementals = store.load_supplementals().unwrap();

        assert_eq!(supplementals.len(), 1);
        assert_eq!(supplementals[0].id, supplemental.id);
        assert_eq!(supplementals[0].kind, "slide-analysis");

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn migration_ledger_records_current_schema_version() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let version: i64 = store
            .conn
            .query_row(
                "SELECT version FROM schema_migrations WHERE version = ?1",
                [CURRENT_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, CURRENT_SCHEMA_VERSION);

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn open_records_partition_initialize_with_pinned_keyspecs() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();

        let (event_type, leaf_id, routing, identity): (String, String, String, String) = store
            .conn
            .query_row(
                "SELECT event_type, leaf_id, routing_keyspec_json, identity_keyspec_json
                 FROM partition_log
                 WHERE event_type = 'initialize'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();

        assert_eq!(event_type, "initialize");
        assert!(leaf_id.starts_with("lake:"));
        assert_eq!(
            routing,
            crate::runtime::partition::routing_keyspec_json().unwrap()
        );
        assert_eq!(
            identity,
            crate::runtime::partition::identity_keyspec_json().unwrap()
        );

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn split_prepare_is_logged_without_changing_replayed_tree() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let root: String = store
            .conn
            .query_row(
                "SELECT leaf_id FROM partition_log WHERE event_type = 'initialize'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let left = format!("lake:{}", uuid::Uuid::now_v7());
        let right = format!("lake:{}", uuid::Uuid::now_v7());

        let seq = store.append_split_prepare(&root, &left, &right).unwrap();
        let tree = store.load_partition_tree().unwrap();

        assert_eq!(seq, 2);
        assert_eq!(tree.current_leaf_ids(), vec![root]);

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn split_commit_records_capacity_bit_and_replays_tree() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let root: String = store
            .conn
            .query_row(
                "SELECT leaf_id FROM partition_log WHERE event_type = 'initialize'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let left = format!("lake:{}", uuid::Uuid::now_v7());
        let right = format!("lake:{}", uuid::Uuid::now_v7());

        let seq = store.append_split_commit(&root, &left, &right, 2).unwrap();
        let (bit_index, reason): (i64, String) = store
            .conn
            .query_row(
                "SELECT bit_index, reason FROM partition_log WHERE event_seq = ?1",
                [seq],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let tree = store.load_partition_tree().unwrap();

        assert_eq!(bit_index, 2);
        assert_eq!(reason, "capacity");
        assert_eq!(tree.current_leaf_ids(), vec![left, right]);

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn failover_and_recover_events_record_control_plane_boundaries() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let root: String = store
            .conn
            .query_row(
                "SELECT leaf_id FROM partition_log WHERE event_type = 'initialize'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let failover_id = format!("spool:{}", uuid::Uuid::now_v7());

        let failover_seq = store.append_failover(&root, &failover_id).unwrap();
        let recover_seq = store.append_recover(&root, &failover_id).unwrap();
        let events = store
            .conn
            .prepare("SELECT event_seq, event_type FROM partition_log ORDER BY event_seq")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(failover_seq, 2);
        assert_eq!(recover_seq, 3);
        assert_eq!(events[1].1, "failover");
        assert_eq!(events[2].1, "recover");

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn partition_log_is_append_only() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();

        let err = store
            .conn
            .execute(
                "UPDATE partition_log SET event_json = '{}' WHERE event_type = 'initialize'",
                [],
            )
            .unwrap_err();
        assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn garbage_collect_orphan_blobs_removes_unreferenced_files() {
        let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
        let db = tmp.join("test.sqlite3");
        let blob_dir = tmp.join("blobs");
        let store = SqlitePersistence::open(&db, &blob_dir).unwrap();
        let orphan = blob_dir.join("f".repeat(64));
        fs::write(&orphan, b"orphan").unwrap();

        let removed = store.garbage_collect_orphan_blobs().unwrap();
        assert_eq!(removed, 1);
        assert!(!orphan.exists());

        let _ = fs::remove_dir_all(tmp);
    }
}
