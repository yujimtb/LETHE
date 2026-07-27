use lethe_core::domain::DataSpaceId;
use lethe_storage_api::{StorageError, StorageResult};
use postgres::{Client, Transaction};
use sha2::{Digest, Sha256};

use super::{quote_identifier, validate_identifier};

const INITIAL_SCHEMA_SQL: &str = include_str!("../migrations/general/0001_initial.sql");
const TRANSACTIONAL_OBSERVATION_APPEND_SEQ_SQL: &str =
    include_str!("../migrations/general/0002_transactional_observation_append_seq.sql");

#[derive(Debug, Clone, PartialEq, Eq)]
struct Migration {
    version: i32,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial_general_lake",
        sql: INITIAL_SCHEMA_SQL,
    },
    Migration {
        version: 2,
        name: "transactional_observation_append_seq",
        sql: TRANSACTIONAL_OBSERVATION_APPEND_SEQ_SQL,
    },
];

/// Result of opening the embedded PostgreSQL migration ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationOutcome {
    /// Versions applied by this invocation, in order.
    pub applied_versions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredMigration {
    version: i32,
    name: String,
    checksum_sha256: String,
}

/// Apply the exact embedded general-Lake PostgreSQL schema.
///
/// The caller must provide one already connected client. This function checks
/// the current role and required schema before any migration object is
/// exposed. Migration names and SHA-256 values are immutable.
///
/// # Errors
///
/// Returns [`StorageError::Invariant`] for role, schema, version, name,
/// checksum, or data-space pin mismatches. PostgreSQL failures are returned as
/// [`StorageError::Backend`].
pub fn apply_general_migrations(
    client: &mut Client,
    schema: &str,
    expected_role: &str,
    data_space_id: &DataSpaceId,
) -> StorageResult<MigrationOutcome> {
    validate_identifier("schema", schema)
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
    validate_identifier("expected_role", expected_role)
        .map_err(|error| StorageError::Invariant(error.to_string()))?;
    if data_space_id.as_str().trim().is_empty() {
        return Err(StorageError::Invariant(
            "data_space_id must not be blank".to_owned(),
        ));
    }
    admit_role_and_schema(client, schema, expected_role)?;
    set_search_path(client, schema)?;
    client
        .batch_execute(
            "
            CREATE TABLE IF NOT EXISTS lethe_schema_migrations (
                version INTEGER PRIMARY KEY CHECK (version > 0),
                name TEXT NOT NULL CHECK (length(btrim(name)) > 0),
                checksum_sha256 TEXT NOT NULL
                    CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
                applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            ",
        )
        .map_err(backend)?;

    let mut transaction = client.transaction().map_err(backend)?;
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&format!("lethe-general-migrations:{schema}")],
        )
        .map_err(backend)?;
    let stored = load_ledger(&mut transaction)?;
    validate_ledger(&stored)?;

    let mut applied_versions = Vec::new();
    for migration in MIGRATIONS.iter().skip(stored.len()) {
        transaction.batch_execute(migration.sql).map_err(backend)?;
        transaction
            .execute(
                "INSERT INTO lethe_schema_migrations (
                    version, name, checksum_sha256
                 ) VALUES ($1, $2, $3)",
                &[
                    &migration.version,
                    &migration.name,
                    &migration_checksum(migration),
                ],
            )
            .map_err(backend)?;
        applied_versions.push(migration.version);
    }
    pin_storage_identity(&mut transaction, data_space_id.as_str(), expected_role)?;
    transaction.commit().map_err(backend)?;
    Ok(MigrationOutcome { applied_versions })
}

fn admit_role_and_schema(
    client: &mut Client,
    schema: &str,
    expected_role: &str,
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
    Ok(())
}

fn set_search_path(client: &mut Client, schema: &str) -> StorageResult<()> {
    client
        .batch_execute(&format!(
            "SET search_path TO {}, pg_catalog",
            quote_identifier(schema)
        ))
        .map_err(backend)
}

fn load_ledger(transaction: &mut Transaction<'_>) -> StorageResult<Vec<StoredMigration>> {
    transaction
        .query(
            "SELECT version, name, checksum_sha256
             FROM lethe_schema_migrations
             ORDER BY version",
            &[],
        )
        .map_err(backend)?
        .into_iter()
        .map(|row| {
            Ok(StoredMigration {
                version: row.get(0),
                name: row.get(1),
                checksum_sha256: row.get(2),
            })
        })
        .collect()
}

fn validate_ledger(stored: &[StoredMigration]) -> StorageResult<()> {
    if let Some(newer) = stored
        .iter()
        .find(|record| record.version as usize > MIGRATIONS.len())
    {
        return Err(StorageError::Invariant(format!(
            "database schema migration version {} is newer than binary version {}",
            newer.version,
            MIGRATIONS.len()
        )));
    }
    for (index, record) in stored.iter().enumerate() {
        let expected_version = i32::try_from(index + 1).map_err(|_| {
            StorageError::Invariant("migration ledger index exceeds INTEGER".to_owned())
        })?;
        if record.version != expected_version {
            return Err(StorageError::Invariant(format!(
                "migration ledger is not contiguous: expected version {expected_version}, got {}",
                record.version
            )));
        }
        let migration = &MIGRATIONS[index];
        if record.name != migration.name {
            return Err(StorageError::Invariant(format!(
                "migration version {} is named {:?}, expected {:?}",
                record.version, record.name, migration.name
            )));
        }
        let expected_checksum = migration_checksum(migration);
        if record.checksum_sha256 != expected_checksum {
            return Err(StorageError::Invariant(format!(
                "migration version {} checksum differs: stored={}, expected={expected_checksum}",
                record.version, record.checksum_sha256
            )));
        }
    }
    Ok(())
}

fn pin_storage_identity(
    transaction: &mut Transaction<'_>,
    data_space_id: &str,
    expected_role: &str,
) -> StorageResult<()> {
    transaction
        .execute(
            "INSERT INTO general_storage_pin (
                singleton, data_space_id, database_role
             ) VALUES (TRUE, $1, $2)
             ON CONFLICT (singleton) DO NOTHING",
            &[&data_space_id, &expected_role],
        )
        .map_err(backend)?;
    let row = transaction
        .query_one(
            "SELECT data_space_id, database_role
             FROM general_storage_pin WHERE singleton",
            &[],
        )
        .map_err(backend)?;
    let pinned_data_space: String = row.get(0);
    let pinned_role: String = row.get(1);
    if pinned_data_space != data_space_id || pinned_role != expected_role {
        return Err(StorageError::Invariant(format!(
            "general storage pin mismatch: data_space={pinned_data_space:?}, role={pinned_role:?}"
        )));
    }
    Ok(())
}

fn migration_checksum(migration: &Migration) -> String {
    hex::encode(Sha256::digest(migration.sql.as_bytes()))
}

fn backend(error: postgres::Error) -> StorageError {
    StorageError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_migration_checksum_is_lowercase_sha256() {
        let checksum = migration_checksum(&MIGRATIONS[0]);
        assert_eq!(checksum.len(), 64);
        assert!(
            checksum
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
    }

    #[test]
    fn exact_ledger_is_accepted() {
        let records = MIGRATIONS.iter().map(stored_record).collect::<Vec<_>>();
        assert!(validate_ledger(&records).is_ok());
    }

    #[test]
    fn changed_checksum_is_rejected() {
        let mut record = stored_record(&MIGRATIONS[0]);
        record.checksum_sha256 = "0".repeat(64);
        assert!(matches!(
            validate_ledger(&[record]),
            Err(StorageError::Invariant(reason)) if reason.contains("checksum differs")
        ));
    }

    #[test]
    fn unknown_newer_version_is_rejected_before_indexing() {
        let mut records = MIGRATIONS.iter().map(stored_record).collect::<Vec<_>>();
        records.push(StoredMigration {
            version: i32::try_from(MIGRATIONS.len() + 1).unwrap(),
            name: "future".to_owned(),
            checksum_sha256: "1".repeat(64),
        });
        assert!(matches!(
            validate_ledger(&records),
            Err(StorageError::Invariant(reason)) if reason.contains("newer than binary")
        ));
    }

    fn stored_record(migration: &Migration) -> StoredMigration {
        StoredMigration {
            version: migration.version,
            name: migration.name.to_owned(),
            checksum_sha256: migration_checksum(migration),
        }
    }
}
