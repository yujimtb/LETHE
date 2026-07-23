use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use lethe_core::domain::{BlobRef, DataSpaceId, OperationalEventId};
use lethe_storage_api::{
    BlobStore, OperationalAppendOutcome, OperationalAppendRequest, OperationalEventFilter,
    OperationalEventStats, OperationalEventStore, StorageError, StorageResult,
    StoredOperationalEvent,
};
use postgres::{Client, NoTls, Transaction, types::ToSql};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum PostgresOperationalStoreError {
    #[error("postgres error: {0}")]
    Postgres(#[from] postgres::Error),
    #[error("configuration invariant violation: {0}")]
    Configuration(String),
}

pub struct PostgresOperationalEventStore {
    write_client: Mutex<Client>,
    read_clients: Vec<Mutex<Client>>,
    next_read_client: AtomicUsize,
    data_space_id: DataSpaceId,
    schema: String,
    role: String,
}

impl PostgresOperationalEventStore {
    pub fn connect_no_tls(
        data_space_id: DataSpaceId,
        dsn: &str,
        schema: &str,
        expected_role: &str,
    ) -> Result<Self, PostgresOperationalStoreError> {
        validate_non_blank("data_space_id", data_space_id.as_str())?;
        validate_non_blank("dsn", dsn)?;
        validate_identifier("schema", schema)?;
        validate_identifier("expected_role", expected_role)?;

        let mut client = Client::connect(dsn, NoTls)?;
        let current_role: String = client.query_one("SELECT current_user", &[])?.get(0);
        if current_role != expected_role {
            return Err(PostgresOperationalStoreError::Configuration(format!(
                "connected role {current_role} does not match required role {expected_role}"
            )));
        }
        let schema_exists: bool = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_namespace WHERE nspname = $1
                 )",
                &[&schema],
            )?
            .get(0);
        if !schema_exists {
            return Err(PostgresOperationalStoreError::Configuration(format!(
                "required schema {schema} does not exist"
            )));
        }
        let search_path = format!(
            "SET search_path TO {}, pg_catalog",
            quote_identifier(schema)
        );
        client.batch_execute(&search_path)?;
        client.batch_execute(
            "
            CREATE TABLE IF NOT EXISTS operational_data_space (
                singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
                data_space_id TEXT NOT NULL CHECK (length(btrim(data_space_id)) > 0)
            );

            CREATE TABLE IF NOT EXISTS operational_observations (
                observation_id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                observation_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS operational_events (
                cursor BIGSERIAL PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                data_space_id TEXT NOT NULL CHECK (length(btrim(data_space_id)) > 0),
                stream_id TEXT NOT NULL CHECK (length(btrim(stream_id)) > 0),
                stream_version BIGINT NOT NULL CHECK (stream_version > 0),
                idempotency_key TEXT NOT NULL CHECK (length(btrim(idempotency_key)) > 0),
                event_type TEXT NOT NULL CHECK (length(btrim(event_type)) > 0),
                actor_id TEXT,
                causation_id TEXT,
                correlation_id TEXT,
                occurred_at TEXT NOT NULL,
                observation_id TEXT NOT NULL UNIQUE
                    REFERENCES operational_observations(observation_id),
                event_sha256 TEXT NOT NULL CHECK (length(event_sha256) = 64),
                event_json TEXT NOT NULL,
                UNIQUE (data_space_id, stream_id, stream_version),
                UNIQUE (data_space_id, idempotency_key)
            );

            CREATE INDEX IF NOT EXISTS operational_events_stream
                ON operational_events(data_space_id, stream_id, stream_version);
            CREATE INDEX IF NOT EXISTS operational_events_stream_cursor
                ON operational_events(data_space_id, stream_id, cursor);

            ALTER TABLE operational_events ADD COLUMN IF NOT EXISTS actor_id TEXT;
            ALTER TABLE operational_events ADD COLUMN IF NOT EXISTS causation_id TEXT;
            ALTER TABLE operational_events ADD COLUMN IF NOT EXISTS correlation_id TEXT;
            UPDATE operational_events
            SET actor_id = NULLIF(event_json::jsonb ->> 'actor_id', '')
            WHERE actor_id IS NULL;
            UPDATE operational_events
            SET causation_id = NULLIF(event_json::jsonb ->> 'causation_id', '')
            WHERE causation_id IS NULL;
            UPDATE operational_events
            SET correlation_id = NULLIF(event_json::jsonb ->> 'correlation_id', '')
            WHERE correlation_id IS NULL;
            CREATE INDEX IF NOT EXISTS operational_events_correlation_cursor
                ON operational_events(data_space_id, correlation_id, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_causation_cursor
                ON operational_events(data_space_id, causation_id, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_type_cursor
                ON operational_events(data_space_id, event_type, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_actor_cursor
                ON operational_events(data_space_id, actor_id, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_occurred_cursor
                ON operational_events(data_space_id, occurred_at, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_stream_occurred_cursor
                ON operational_events(data_space_id, stream_id, occurred_at, cursor);

            CREATE TABLE IF NOT EXISTS operational_event_stats (
                data_space_id TEXT PRIMARY KEY,
                event_count BIGINT NOT NULL CHECK (event_count >= 0),
                max_cursor BIGINT NOT NULL CHECK (max_cursor >= 0)
            );

            CREATE TABLE IF NOT EXISTS operational_blobs (
                blob_ref TEXT PRIMARY KEY,
                bytes BIGINT NOT NULL CHECK (bytes >= 0),
                content BYTEA NOT NULL
            );

            CREATE OR REPLACE FUNCTION reject_operational_event_mutation()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'operational_events is append-only';
            END;
            $$;

            DROP TRIGGER IF EXISTS operational_events_no_update
                ON operational_events;
            CREATE TRIGGER operational_events_no_update
                BEFORE UPDATE OR DELETE ON operational_events
                FOR EACH ROW EXECUTE FUNCTION reject_operational_event_mutation();
            ",
        )?;
        client.execute(
            "INSERT INTO operational_data_space (singleton, data_space_id)
             VALUES (1, $1) ON CONFLICT (singleton) DO NOTHING",
            &[&data_space_id.as_str()],
        )?;
        let pinned: String = client
            .query_one(
                "SELECT data_space_id FROM operational_data_space WHERE singleton = 1",
                &[],
            )?
            .get(0);
        if pinned != data_space_id.as_str() {
            return Err(PostgresOperationalStoreError::Configuration(format!(
                "postgres schema {schema} is pinned to data space {pinned}, not {data_space_id}"
            )));
        }
        let mut read_clients = Vec::with_capacity(4);
        for _ in 0..4 {
            let mut read_client = Client::connect(dsn, NoTls)?;
            read_client.batch_execute(&search_path)?;
            read_clients.push(Mutex::new(read_client));
        }
        client.execute(
            "INSERT INTO operational_event_stats (data_space_id, event_count, max_cursor)
             SELECT $1, COUNT(*), COALESCE(MAX(cursor), 0)
             FROM operational_events WHERE data_space_id = $1
             ON CONFLICT (data_space_id) DO NOTHING",
            &[&data_space_id.as_str()],
        )?;
        Ok(Self {
            write_client: Mutex::new(client),
            read_clients,
            next_read_client: AtomicUsize::new(0),
            data_space_id,
            schema: schema.to_owned(),
            role: expected_role.to_owned(),
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn role(&self) -> &str {
        &self.role
    }

    fn client(&self) -> StorageResult<MutexGuard<'_, Client>> {
        self.write_client
            .lock()
            .map_err(|_| StorageError::Backend("postgres client mutex is poisoned".to_owned()))
    }

    fn read_client(&self) -> StorageResult<MutexGuard<'_, Client>> {
        let index = self.next_read_client.fetch_add(1, Ordering::Relaxed) % self.read_clients.len();
        self.read_clients[index]
            .lock()
            .map_err(|_| StorageError::Backend("postgres read client mutex is poisoned".to_owned()))
    }

    fn backend(error: impl std::fmt::Display) -> StorageError {
        StorageError::Backend(error.to_string())
    }
}

fn unsupported_history_v2_append_error() -> StorageError {
    StorageError::Backend(
        "history v2 operational append requires a backend with cutover bridge support; use the SQLite Personal Lake backend"
            .to_owned(),
    )
}

impl OperationalEventStore for PostgresOperationalEventStore {
    fn data_space_id(&self) -> &DataSpaceId {
        &self.data_space_id
    }

    fn append_operational_events(
        &self,
        requests: &[OperationalAppendRequest],
    ) -> StorageResult<Vec<OperationalAppendOutcome>> {
        for request in requests {
            request.event.validate()?;
            if request.event.data_space_id != self.data_space_id {
                return Err(StorageError::Invariant(format!(
                    "event data space {} does not match postgres Lake {}",
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
        let mut client = self.client()?;
        let mut transaction = client.transaction().map_err(Self::backend)?;
        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            outcomes.push(append_one(&mut transaction, &self.data_space_id, request)?);
        }
        transaction.commit().map_err(Self::backend)?;
        Ok(outcomes)
    }

    fn append_operational_events_v2_with_bridge(
        &self,
        _source_instance_id: &str,
        _generation: Option<u64>,
        _requests: &[OperationalAppendRequest],
    ) -> StorageResult<Vec<OperationalAppendOutcome>> {
        Err(unsupported_history_v2_append_error())
    }

    fn operational_event_stats(&self) -> StorageResult<OperationalEventStats> {
        let mut client = self.read_client()?;
        let row = client
            .query_one(
                "SELECT event_count, max_cursor
                 FROM operational_event_stats WHERE data_space_id = $1",
                &[&self.data_space_id.as_str()],
            )
            .map_err(Self::backend)?;
        Ok(OperationalEventStats {
            count: from_i64("event count", row.get(0))?,
            max_cursor: from_i64("max cursor", row.get(1))?,
        })
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
        let after = to_i64("after_cursor", after_cursor)?;
        let limit = to_i64("limit", limit as u64)?;
        let mut client = self.read_client()?;
        let rows = client
            .query(
                "SELECT cursor, event_json FROM operational_events
                 WHERE data_space_id = $1 AND cursor > $2
                 ORDER BY cursor LIMIT $3",
                &[&self.data_space_id.as_str(), &after, &limit],
            )
            .map_err(Self::backend)?;
        stored_rows(rows)
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
                "occurred_at_from must not be later than occurred_at_to".to_owned(),
            ));
        }
        let mut values: Vec<Box<dyn ToSql + Sync>> = vec![
            Box::new(self.data_space_id.as_str().to_owned()),
            Box::new(to_i64("after_cursor", after_cursor)?),
        ];
        let mut predicates = vec!["data_space_id = $1".to_owned(), "cursor > $2".to_owned()];
        let mut add_text = |column: &str, value: Option<&str>| -> StorageResult<()> {
            if let Some(value) = value {
                require_filter_value(column, value)?;
                values.push(Box::new(value.to_owned()));
                predicates.push(format!("{column} = ${}", values.len()));
            }
            Ok(())
        };
        add_text("correlation_id", filter.correlation_id.as_deref())?;
        add_text(
            "causation_id",
            filter.causation_id.as_ref().map(OperationalEventId::as_str),
        )?;
        add_text("event_type", filter.event_type.as_deref())?;
        add_text("stream_id", filter.stream_id.as_deref())?;
        add_text("actor_id", filter.actor_id.as_deref())?;
        if let Some(from) = filter.occurred_at_from {
            values.push(Box::new(from.to_rfc3339()));
            predicates.push(format!("occurred_at >= ${}", values.len()));
        }
        if let Some(to) = filter.occurred_at_to {
            values.push(Box::new(to.to_rfc3339()));
            predicates.push(format!("occurred_at <= ${}", values.len()));
        }
        let limit_position = values.len() + 1;
        values.push(Box::new(to_i64("limit", limit as u64)?));
        let query = format!(
            "SELECT cursor, event_json FROM operational_events WHERE {} ORDER BY cursor LIMIT ${limit_position}",
            predicates.join(" AND ")
        );
        let params = values
            .iter()
            .map(|value| value.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let mut client = self.read_client()?;
        let rows = client.query(&query, &params).map_err(Self::backend)?;
        stored_rows(rows)
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
        let after = to_i64("after_stream_version", after_stream_version)?;
        let limit = to_i64("limit", limit as u64)?;
        let mut client = self.read_client()?;
        let rows = client
            .query(
                "SELECT cursor, event_json FROM operational_events
                 WHERE data_space_id = $1 AND stream_id = $2 AND stream_version > $3
                 ORDER BY stream_version LIMIT $4",
                &[&self.data_space_id.as_str(), &stream_id, &after, &limit],
            )
            .map_err(Self::backend)?;
        stored_rows(rows)
    }

    fn operational_event_by_id(
        &self,
        event_id: &OperationalEventId,
    ) -> StorageResult<Option<StoredOperationalEvent>> {
        let mut client = self.read_client()?;
        let row = client
            .query_opt(
                "SELECT cursor, event_json FROM operational_events
                 WHERE data_space_id = $1 AND event_id = $2",
                &[&self.data_space_id.as_str(), &event_id.as_str()],
            )
            .map_err(Self::backend)?;
        row.map(stored_row).transpose()
    }

    fn operational_stream_version(&self, stream_id: &str) -> StorageResult<u64> {
        if stream_id.trim().is_empty() {
            return Err(StorageError::Invariant(
                "stream_id must not be blank".to_owned(),
            ));
        }
        let mut client = self.read_client()?;
        let value: i64 = client
            .query_one(
                "SELECT COALESCE(MAX(stream_version), 0)
                 FROM operational_events
                 WHERE data_space_id = $1 AND stream_id = $2",
                &[&self.data_space_id.as_str(), &stream_id],
            )
            .map_err(Self::backend)?
            .get(0);
        from_i64("stream_version", value)
    }
}

impl BlobStore for PostgresOperationalEventStore {
    fn put_blob(&self, data: &[u8], max_bytes: usize) -> StorageResult<BlobRef> {
        if data.len() > max_bytes {
            return Err(StorageError::Invariant(format!(
                "blob size {} exceeds maximum {max_bytes}",
                data.len()
            )));
        }
        let digest = hex::encode(Sha256::digest(data));
        let blob_ref = BlobRef::new(format!("blob:sha256:{digest}"));
        let bytes = to_i64("blob bytes", data.len() as u64)?;
        let mut client = self.client()?;
        client
            .execute(
                "INSERT INTO operational_blobs (blob_ref, bytes, content)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (blob_ref) DO NOTHING",
                &[&blob_ref.as_str(), &bytes, &data],
            )
            .map_err(Self::backend)?;
        Ok(blob_ref)
    }

    fn put_blobs(&self, data: &[&[u8]], max_bytes: usize) -> StorageResult<Vec<BlobRef>> {
        if let Some((index, blob)) = data
            .iter()
            .enumerate()
            .find(|(_, blob)| blob.len() > max_bytes)
        {
            return Err(StorageError::Invariant(format!(
                "blob at batch index {index} has size {} exceeding maximum {max_bytes}",
                blob.len()
            )));
        }
        let blobs = data
            .iter()
            .map(|blob| {
                let digest = hex::encode(Sha256::digest(blob));
                let blob_ref = BlobRef::new(format!("blob:sha256:{digest}"));
                let bytes = to_i64("blob bytes", blob.len() as u64)?;
                Ok((blob_ref, bytes, *blob))
            })
            .collect::<StorageResult<Vec<_>>>()?;

        let mut client = self.client()?;
        let mut transaction = client.transaction().map_err(Self::backend)?;
        {
            let statement = transaction
                .prepare(
                    "INSERT INTO operational_blobs (blob_ref, bytes, content)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (blob_ref) DO NOTHING",
                )
                .map_err(Self::backend)?;
            for (blob_ref, bytes, content) in &blobs {
                transaction
                    .execute(&statement, &[&blob_ref.as_str(), bytes, content])
                    .map_err(Self::backend)?;
            }
        }
        transaction.commit().map_err(Self::backend)?;
        Ok(blobs.into_iter().map(|(blob_ref, _, _)| blob_ref).collect())
    }

    fn get_blob(&self, blob_ref: &BlobRef) -> StorageResult<Option<Vec<u8>>> {
        let mut client = self.read_client()?;
        client
            .query_opt(
                "SELECT content FROM operational_blobs WHERE blob_ref = $1",
                &[&blob_ref.as_str()],
            )
            .map_err(Self::backend)
            .map(|row| row.map(|row| row.get(0)))
    }
}

fn append_one(
    transaction: &mut Transaction<'_>,
    data_space_id: &DataSpaceId,
    request: &OperationalAppendRequest,
) -> StorageResult<OperationalAppendOutcome> {
    let event_json =
        serde_json::to_string(&request.event).map_err(PostgresOperationalEventStore::backend)?;
    let event_sha256 = hex::encode(Sha256::digest(event_json.as_bytes()));
    let idempotency_key = request.event.observation.idempotency_key.as_str();
    if let Some(row) = transaction
        .query_opt(
            "SELECT cursor, stream_version, event_sha256, event_id
             FROM operational_events
             WHERE data_space_id = $1 AND idempotency_key = $2",
            &[&data_space_id.as_str(), &idempotency_key],
        )
        .map_err(PostgresOperationalEventStore::backend)?
    {
        let cursor = from_i64("cursor", row.get(0))?;
        let stream_version = from_i64("stream_version", row.get(1))?;
        let stored_sha256: String = row.get(2);
        let stored_event_id: String = row.get(3);
        if stored_sha256 != event_sha256 || stored_event_id != request.event.event_id.as_str() {
            return Err(StorageError::OperationalIdempotencyCollision(
                idempotency_key.to_owned(),
            ));
        }
        return Ok(OperationalAppendOutcome::Duplicate {
            cursor,
            stream_version,
        });
    }
    if let Some(row) = transaction
        .query_opt(
            "SELECT cursor, event_sha256 FROM operational_events WHERE event_id = $1",
            &[&request.event.event_id.as_str()],
        )
        .map_err(PostgresOperationalEventStore::backend)?
    {
        let cursor = from_i64("cursor", row.get(0))?;
        let stored_sha256: String = row.get(1);
        if stored_sha256 != event_sha256 {
            return Err(StorageError::OperationalEventIdCollision(
                request.event.event_id.as_str().to_owned(),
            ));
        }
        return Ok(OperationalAppendOutcome::Duplicate {
            cursor,
            stream_version: request.event.stream_version,
        });
    }
    let advisory_key = format!("{}:{}", data_space_id.as_str(), request.event.stream_id);
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&advisory_key],
        )
        .map_err(PostgresOperationalEventStore::backend)?;
    let actual: i64 = transaction
        .query_one(
            "SELECT COALESCE(MAX(stream_version), 0)
             FROM operational_events
             WHERE data_space_id = $1 AND stream_id = $2",
            &[&data_space_id.as_str(), &request.event.stream_id],
        )
        .map_err(PostgresOperationalEventStore::backend)?
        .get(0);
    let actual = from_i64("stream_version", actual)?;
    if actual != request.expected_stream_version {
        return Ok(OperationalAppendOutcome::VersionConflict {
            expected: request.expected_stream_version,
            actual,
        });
    }

    let observation_json = serde_json::to_string(&request.event.observation)
        .map_err(PostgresOperationalEventStore::backend)?;
    transaction
        .execute(
            "INSERT INTO operational_observations (
                observation_id, idempotency_key, observation_json
             ) VALUES ($1, $2, $3)",
            &[
                &request.event.observation.id.as_str(),
                &idempotency_key,
                &observation_json,
            ],
        )
        .map_err(PostgresOperationalEventStore::backend)?;
    let stream_version = to_i64("stream_version", request.event.stream_version)?;
    let row = transaction
        .query_one(
            "INSERT INTO operational_events (
                event_id, data_space_id, stream_id, stream_version,
                idempotency_key, event_type, actor_id, causation_id,
                correlation_id, occurred_at, observation_id, event_sha256,
                event_json
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             RETURNING cursor",
            &[
                &request.event.event_id.as_str(),
                &data_space_id.as_str(),
                &request.event.stream_id,
                &stream_version,
                &idempotency_key,
                &request.event.event_type,
                &request.event.actor_id,
                &request
                    .event
                    .causation_id
                    .as_ref()
                    .map(OperationalEventId::as_str),
                &request.event.correlation_id,
                &request.event.occurred_at.to_rfc3339(),
                &request.event.observation.id.as_str(),
                &event_sha256,
                &event_json,
            ],
        )
        .map_err(PostgresOperationalEventStore::backend)?;
    let cursor = from_i64("cursor", row.get(0))?;
    let cursor_value = to_i64("cursor", cursor)?;
    transaction
        .execute(
            "UPDATE operational_event_stats
             SET event_count = event_count + 1,
                 max_cursor = GREATEST(max_cursor, $2)
             WHERE data_space_id = $1",
            &[&data_space_id.as_str(), &cursor_value],
        )
        .map_err(PostgresOperationalEventStore::backend)?;
    Ok(OperationalAppendOutcome::Appended {
        cursor,
        stream_version: request.event.stream_version,
    })
}

fn stored_rows(rows: Vec<postgres::Row>) -> StorageResult<Vec<StoredOperationalEvent>> {
    rows.into_iter().map(stored_row).collect()
}

fn stored_row(row: postgres::Row) -> StorageResult<StoredOperationalEvent> {
    let cursor = from_i64("cursor", row.get(0))?;
    let event_json: String = row.get(1);
    let event =
        serde_json::from_str(&event_json).map_err(PostgresOperationalEventStore::backend)?;
    Ok(StoredOperationalEvent { cursor, event })
}

fn require_filter_value(field: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        return Err(StorageError::Invariant(format!(
            "operational event filter {field} must not be blank"
        )));
    }
    Ok(())
}

fn validate_non_blank(field: &str, value: &str) -> Result<(), PostgresOperationalStoreError> {
    if value.trim().is_empty() {
        return Err(PostgresOperationalStoreError::Configuration(format!(
            "{field} must not be blank"
        )));
    }
    Ok(())
}

fn validate_identifier(field: &str, value: &str) -> Result<(), PostgresOperationalStoreError> {
    validate_non_blank(field, value)?;
    let mut chars = value.chars();
    if !chars
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(PostgresOperationalStoreError::Configuration(format!(
            "{field} must be an unquoted PostgreSQL identifier"
        )));
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn to_i64(field: &str, value: u64) -> StorageResult<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} exceeds PostgreSQL BIGINT")))
}

fn from_i64(field: &str, value: i64) -> StorageResult<u64> {
    u64::try_from(value)
        .map_err(|_| StorageError::Invariant(format!("{field} must not be negative")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_restricted_before_sql_construction() {
        assert!(validate_identifier("schema", "space_personal").is_ok());
        assert!(validate_identifier("schema", "space-personal").is_err());
        assert!(validate_identifier("schema", "space;drop schema public").is_err());
    }

    #[test]
    fn postgres_history_v2_bridge_append_is_explicitly_unsupported() {
        let error = unsupported_history_v2_append_error();
        assert!(matches!(
            error,
            StorageError::Backend(reason)
                if reason.contains("history v2 operational append requires a backend with cutover bridge support")
                    && reason.contains("SQLite Personal Lake backend")
        ));
    }

    #[test]
    #[ignore = "requires LETHE_TEST_POSTGRES_DSN, schema and role provisioning"]
    fn postgres_operational_store_conformance() {
        let dsn = std::env::var("LETHE_TEST_POSTGRES_DSN").unwrap();
        let schema = std::env::var("LETHE_TEST_POSTGRES_SCHEMA").unwrap();
        let role = std::env::var("LETHE_TEST_POSTGRES_ROLE").unwrap();
        let store = PostgresOperationalEventStore::connect_no_tls(
            DataSpaceId::new("space:postgres-conformance"),
            &dsn,
            &schema,
            &role,
        )
        .unwrap();
        lethe_storage_api::conformance::operational_event_store_round_trip(&store);
        lethe_storage_api::conformance::blob_store_round_trip(&store);
    }
}
