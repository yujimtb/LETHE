//! Dedicated failover spool for a lost leaf.

use std::fs;
use std::path::Path;

use rusqlite::{Connection, params};

use crate::domain::Observation;
use crate::self_host::persistence::{
    DurableAppendOutcome, PersistenceError, RehomeMode, SqlitePersistence,
};

const CANONICAL_JSON_META_KEY: &str = "canonical_json";

pub struct FailoverSpool {
    conn: Connection,
}

impl FailoverSpool {
    pub fn open(database_path: &Path) -> Result<Self, PersistenceError> {
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(database_path)?;
        let spool = Self { conn };
        spool.init_schema()?;
        Ok(spool)
    }

    pub fn append(&self, observation: &Observation) -> Result<i64, PersistenceError> {
        let canonical_json = observation
            .meta
            .get(CANONICAL_JSON_META_KEY)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                PersistenceError::SchemaInvariant(
                    "failover spool requires observation.meta.canonical_json".to_owned(),
                )
            })?;
        let observation_json = serde_json::to_string(observation)?;

        self.conn.execute(
            "INSERT INTO spool_entries (
                id,
                identity_key,
                canonical_json,
                published,
                recorded_at,
                observation_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                observation.id.as_str(),
                observation.idempotency_key.as_str(),
                canonical_json,
                observation.published.to_rfc3339(),
                observation.recorded_at.to_rfc3339(),
                observation_json,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn drain_into(
        &self,
        destination: &SqlitePersistence,
    ) -> Result<Vec<DurableAppendOutcome>, PersistenceError> {
        let observations = self.load_ordered()?;
        let mut outcomes = Vec::with_capacity(observations.len());
        for observation in observations {
            outcomes
                .push(destination.rehome_observation(&observation, RehomeMode::StoredIdentity)?);
        }
        self.retire()?;
        Ok(outcomes)
    }

    pub fn load_ordered(&self) -> Result<Vec<Observation>, PersistenceError> {
        let mut stmt = self
            .conn
            .prepare("SELECT observation_json FROM spool_entries ORDER BY spool_seq")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut observations = Vec::new();
        for row in rows {
            observations.push(serde_json::from_str::<Observation>(&row?)?);
        }
        Ok(observations)
    }

    pub fn is_retired(&self) -> Result<bool, PersistenceError> {
        self.conn
            .query_row(
                "SELECT retired FROM spool_metadata WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|retired| retired == 1)
            .map_err(PersistenceError::from)
    }

    fn retire(&self) -> Result<(), PersistenceError> {
        self.conn.execute(
            "UPDATE spool_metadata SET retired = 1, retired_at = ?1 WHERE id = 1",
            [chrono::Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    fn init_schema(&self) -> Result<(), PersistenceError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS spool_entries (
                spool_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL,
                identity_key TEXT NOT NULL,
                canonical_json TEXT NOT NULL,
                published TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                observation_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS spool_metadata (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                retired INTEGER NOT NULL CHECK (retired IN (0, 1)),
                retired_at TEXT
            );

            INSERT OR IGNORE INTO spool_metadata (id, retired, retired_at)
            VALUES (1, 0, NULL);

            CREATE TRIGGER IF NOT EXISTS spool_entries_no_update
            BEFORE UPDATE ON spool_entries
            BEGIN
                SELECT RAISE(ABORT, 'failover spool is append-only');
            END;

            CREATE TRIGGER IF NOT EXISTS spool_entries_no_delete
            BEFORE DELETE ON spool_entries
            BEGIN
                SELECT RAISE(ABORT, 'failover spool is append-only');
            END;
            ",
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    use crate::domain::{
        AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Observation, ObserverRef,
        SchemaRef, SemVer,
    };

    fn observation(key: &str) -> Observation {
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
            payload: serde_json::json!({ "key": key }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({
                CANONICAL_JSON_META_KEY: serde_json::json!({
                    "source": "test",
                    "object_id": key,
                    "body": "same"
                }).to_string(),
            }),
        }
    }

    #[test]
    fn spool_keeps_duplicates_and_drain_deduplicates_at_destination() {
        let tmp = std::env::temp_dir().join(format!("lethe-spool-test-{}", uuid::Uuid::now_v7()));
        let spool = FailoverSpool::open(&tmp.join("spool.sqlite3")).unwrap();
        let destination =
            SqlitePersistence::open(&tmp.join("leaf.sqlite3"), &tmp.join("blobs")).unwrap();
        let first = observation("dup-key");
        let mut duplicate = first.clone();
        duplicate.id = Observation::new_id();

        assert_eq!(spool.append(&first).unwrap(), 1);
        assert_eq!(spool.append(&duplicate).unwrap(), 2);
        let outcomes = spool.drain_into(&destination).unwrap();

        assert_eq!(
            outcomes,
            vec![
                DurableAppendOutcome::Appended(first.id.clone()),
                DurableAppendOutcome::Duplicate(first.id.clone()),
            ]
        );
        assert!(spool.is_retired().unwrap());

        let _ = fs::remove_dir_all(tmp);
    }

    #[test]
    fn spool_entries_are_append_only() {
        let tmp = std::env::temp_dir().join(format!("lethe-spool-test-{}", uuid::Uuid::now_v7()));
        let spool = FailoverSpool::open(&tmp.join("spool.sqlite3")).unwrap();
        let first = observation("key");
        spool.append(&first).unwrap();

        let err = spool
            .conn
            .execute("DELETE FROM spool_entries WHERE spool_seq = 1", [])
            .unwrap_err();

        assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));

        let _ = fs::remove_dir_all(tmp);
    }
}
