use super::*;

use std::collections::HashSet;

const OBSERVATION_SCHEMA_BACKFILL_BATCH_SIZE: i64 = 512;

impl SqlitePersistence {
    pub(super) fn init_schema(&self) -> Result<(), PersistenceError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS observations (
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

            CREATE TABLE IF NOT EXISTS observation_identity_registry (
                identity_key TEXT PRIMARY KEY,
                observation_id TEXT NOT NULL UNIQUE,
                canonical_json_sha256 TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS operational_data_space (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                data_space_id TEXT NOT NULL CHECK (length(trim(data_space_id)) > 0)
            );

            CREATE TABLE IF NOT EXISTS operational_events (
                cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                data_space_id TEXT NOT NULL CHECK (length(trim(data_space_id)) > 0),
                stream_id TEXT NOT NULL CHECK (length(trim(stream_id)) > 0),
                stream_version INTEGER NOT NULL CHECK (stream_version > 0),
                idempotency_key TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
                event_type TEXT NOT NULL CHECK (length(trim(event_type)) > 0),
                actor_id TEXT,
                causation_id TEXT,
                correlation_id TEXT,
                occurred_at TEXT NOT NULL,
                observation_id TEXT NOT NULL UNIQUE,
                event_sha256 TEXT NOT NULL CHECK (length(event_sha256) = 64),
                event_json TEXT NOT NULL,
                UNIQUE (data_space_id, stream_id, stream_version),
                UNIQUE (data_space_id, idempotency_key),
                FOREIGN KEY (observation_id) REFERENCES observations(id)
            );

            CREATE INDEX IF NOT EXISTS operational_events_stream
                ON operational_events(data_space_id, stream_id, stream_version);
            CREATE INDEX IF NOT EXISTS operational_events_stream_cursor
                ON operational_events(data_space_id, stream_id, cursor);

            CREATE INDEX IF NOT EXISTS operational_events_type_cursor
                ON operational_events(data_space_id, event_type, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_occurred_cursor
                ON operational_events(data_space_id, occurred_at, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_stream_occurred_cursor
                ON operational_events(data_space_id, stream_id, occurred_at, cursor);

            CREATE TRIGGER IF NOT EXISTS operational_events_no_update
            BEFORE UPDATE ON operational_events
            BEGIN
                SELECT RAISE(ABORT, 'operational_events is append-only');
            END;

            CREATE TRIGGER IF NOT EXISTS operational_events_no_delete
            BEFORE DELETE ON operational_events
            BEGIN
                SELECT RAISE(ABORT, 'operational_events is append-only');
            END;

            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS slack_thread_catalog_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                discovery_high_water INTEGER NOT NULL CHECK (discovery_high_water >= 0),
                poll_generation INTEGER NOT NULL CHECK (poll_generation >= 0)
            );

            CREATE TABLE IF NOT EXISTS slack_thread_catalog (
                source_instance TEXT NOT NULL CHECK (length(trim(source_instance)) > 0),
                channel_id TEXT NOT NULL CHECK (length(trim(channel_id)) > 0),
                thread_ts TEXT NOT NULL CHECK (length(trim(thread_ts)) > 0),
                reply_cursor TEXT NOT NULL CHECK (length(trim(reply_cursor)) > 0),
                active INTEGER NOT NULL CHECK (active IN (0, 1)),
                next_poll_generation INTEGER NOT NULL CHECK (next_poll_generation >= 0),
                discovered_append_seq INTEGER NOT NULL CHECK (discovered_append_seq > 0),
                PRIMARY KEY (source_instance, channel_id, thread_ts)
            );

            CREATE INDEX IF NOT EXISTS slack_thread_catalog_poll_queue
                ON slack_thread_catalog (
                    source_instance,
                    channel_id,
                    active,
                    next_poll_generation,
                    thread_ts
                );

            CREATE TABLE IF NOT EXISTS blobs (
                blob_ref TEXT PRIMARY KEY,
                file_name TEXT NOT NULL CHECK (length(file_name) = 64 AND file_name NOT GLOB '*[^0-9a-f]*')
            );

            CREATE TABLE IF NOT EXISTS supplementals (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                supplemental_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS projection_materializations (
                projection_id TEXT PRIMARY KEY,
                records_json TEXT NOT NULL,
                materialized_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS projection_materialization_items (
                projection_id TEXT NOT NULL,
                item_key TEXT NOT NULL CHECK (length(trim(item_key)) > 0),
                owner_key TEXT NOT NULL CHECK (length(trim(owner_key)) > 0),
                sort_key TEXT NOT NULL CHECK (length(trim(sort_key)) > 0),
                value_json TEXT NOT NULL,
                PRIMARY KEY (projection_id, item_key)
            );

            CREATE INDEX IF NOT EXISTS projection_materialization_items_owner_order
                ON projection_materialization_items (
                    projection_id,
                    owner_key,
                    sort_key,
                    item_key
                );

            CREATE TABLE IF NOT EXISTS projection_visible_blob_refs (
                projection_id TEXT NOT NULL,
                item_key TEXT NOT NULL,
                blob_ref TEXT NOT NULL,
                owner_key TEXT NOT NULL,
                consent_scope TEXT NOT NULL,
                PRIMARY KEY (projection_id, item_key, blob_ref)
            );

            CREATE INDEX IF NOT EXISTS projection_visible_blob_refs_lookup
                ON projection_visible_blob_refs (projection_id, blob_ref);

            CREATE TABLE IF NOT EXISTS projection_leaf_watermarks (
                projection_id TEXT NOT NULL,
                leaf_id TEXT NOT NULL CHECK (leaf_id LIKE 'lake:%'),
                append_seq INTEGER NOT NULL CHECK (append_seq >= 0),
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (projection_id, leaf_id)
            );

            CREATE TABLE IF NOT EXISTS dead_letters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_instance TEXT NOT NULL,
                item_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_events (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                actor TEXT NOT NULL,
                event_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_metrics (
                source_instance TEXT PRIMARY KEY,
                fetched INTEGER NOT NULL,
                ingested INTEGER NOT NULL,
                skipped INTEGER NOT NULL,
                failed INTEGER NOT NULL,
                quarantined INTEGER NOT NULL,
                latency_ms INTEGER NOT NULL,
                last_error TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS encrypted_secrets (
                secret_ref TEXT PRIMARY KEY,
                nonce BLOB NOT NULL,
                ciphertext BLOB NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS keyspec_history (
                migration_id TEXT PRIMARY KEY,
                routing_keyspec_version TEXT NOT NULL,
                identity_keyspec_version TEXT NOT NULL,
                partition_log_json TEXT NOT NULL,
                retired_at TEXT NOT NULL
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
        let has_legacy_path = self
            .conn
            .prepare("PRAGMA table_info(blobs)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "file_path");
        if has_legacy_path {
            return Err(PersistenceError::SchemaInvariant(
                "legacy blobs.file_path schema is unsupported; run the explicit offline blob-index migration before starting LETHE".to_owned(),
            ));
        }
        self.ensure_partition_initialize()?;
        self.migrate_existing_schema()?;
        self.apply_schema_migrations()?;
        self.conn.execute_batch(
            "
            CREATE INDEX IF NOT EXISTS observations_leaf_append
                ON observations(leaf_id, append_seq);
            ",
        )?;
        self.conn.execute(
            "INSERT OR IGNORE INTO slack_thread_catalog_state (
                singleton, discovery_high_water, poll_generation
             ) VALUES (1, 0, 0)",
            [],
        )?;
        Ok(())
    }

    fn apply_schema_migrations(&self) -> Result<(), PersistenceError> {
        let identity_lookup_recorded =
            self.schema_migration_recorded(SCHEMA_VERSION_IDENTITY_LOOKUP_INDEX)?;
        let lock_split_recorded =
            self.schema_migration_recorded(SCHEMA_VERSION_LOCK_SPLIT_SCALARS)?;
        let keyset_reads_recorded = self.schema_migration_recorded(SCHEMA_VERSION_KEYSET_READS)?;
        let privacy_projection_recorded =
            self.schema_migration_recorded(SCHEMA_VERSION_PRIVACY_PROJECTION)?;
        let reconsent_privacy_index_recorded =
            self.schema_migration_recorded(SCHEMA_VERSION_RECONSENT_PRIVACY_INDEX)?;
        let cutover_bridge_recorded =
            self.schema_migration_recorded(SCHEMA_VERSION_CUTOVER_BRIDGE)?;

        if lock_split_recorded && !identity_lookup_recorded {
            return Err(PersistenceError::SchemaInvariant(
                "schema migration v10 is recorded without prerequisite v9".to_owned(),
            ));
        }
        if keyset_reads_recorded && !lock_split_recorded {
            return Err(PersistenceError::SchemaInvariant(
                "schema migration v11 is recorded without prerequisite v10".to_owned(),
            ));
        }
        if privacy_projection_recorded && !keyset_reads_recorded {
            return Err(PersistenceError::SchemaInvariant(
                "schema migration v12 is recorded without prerequisite v11".to_owned(),
            ));
        }
        if reconsent_privacy_index_recorded && !privacy_projection_recorded {
            return Err(PersistenceError::SchemaInvariant(
                "schema migration v13 is recorded without prerequisite v12".to_owned(),
            ));
        }
        if cutover_bridge_recorded && !reconsent_privacy_index_recorded {
            return Err(PersistenceError::SchemaInvariant(
                "schema migration v14 is recorded without prerequisite v13".to_owned(),
            ));
        }

        if identity_lookup_recorded {
            self.require_schema_migration_name(
                SCHEMA_VERSION_IDENTITY_LOOKUP_INDEX,
                "observation_identity_lookup_index",
            )?;
            self.require_schema_object("index", "observations_identity_append")?;
        } else {
            self.apply_identity_lookup_index_migration()?;
        }

        if lock_split_recorded {
            self.require_schema_migration_name(
                SCHEMA_VERSION_LOCK_SPLIT_SCALARS,
                "append_commit_lock_split_scalars",
            )?;
            self.require_lock_split_schema_objects()?;
        } else {
            self.apply_lock_split_scalars_migration()?;
        }

        if keyset_reads_recorded {
            self.require_schema_migration_name(
                SCHEMA_VERSION_KEYSET_READS,
                "indexed_keyset_reads",
            )?;
            self.require_keyset_read_schema_objects()?;
        } else {
            self.apply_keyset_reads_migration()?;
        }

        if privacy_projection_recorded {
            self.require_schema_migration_name(
                SCHEMA_VERSION_PRIVACY_PROJECTION,
                "privacy_projection_visibility",
            )?;
            self.require_privacy_projection_schema_objects()?;
        } else {
            self.apply_privacy_projection_migration()?;
        }

        if reconsent_privacy_index_recorded {
            self.require_schema_migration_name(
                SCHEMA_VERSION_RECONSENT_PRIVACY_INDEX,
                "reconsent_privacy_reverse_index",
            )?;
            self.require_reconsent_privacy_index_schema_objects()?;
        } else {
            self.apply_reconsent_privacy_index_migration()?;
        }

        if cutover_bridge_recorded {
            self.require_schema_migration_name(
                SCHEMA_VERSION_CUTOVER_BRIDGE,
                "v1_v2_cutover_bridge",
            )?;
            self.require_cutover_bridge_schema_objects()?;
        } else {
            self.apply_cutover_bridge_migration()?;
        }

        Ok(())
    }

    fn apply_identity_lookup_index_migration(&self) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute(
            "CREATE INDEX IF NOT EXISTS observations_identity_append
             ON observations(identity_key, append_seq)",
            [],
        )?;
        self.backfill_global_identity_registry(&transaction)?;
        record_schema_migration(
            &transaction,
            SCHEMA_VERSION_IDENTITY_LOOKUP_INDEX,
            "observation_identity_lookup_index",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn apply_lock_split_scalars_migration(&self) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS observation_stats (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                observation_count INTEGER NOT NULL CHECK (observation_count >= 0),
                max_append_seq INTEGER NOT NULL CHECK (max_append_seq >= 0)
            );

            CREATE TABLE IF NOT EXISTS projection_manifest_fields (
                projection_id TEXT NOT NULL,
                field_key TEXT NOT NULL CHECK (length(trim(field_key)) > 0),
                value_json TEXT NOT NULL,
                PRIMARY KEY (projection_id, field_key)
            );

            CREATE INDEX IF NOT EXISTS audit_events_timestamp_id
                ON audit_events(timestamp, id);

            CREATE TABLE IF NOT EXISTS operational_event_stats (
                data_space_id TEXT PRIMARY KEY CHECK (length(trim(data_space_id)) > 0),
                event_count INTEGER NOT NULL CHECK (event_count >= 0),
                max_cursor INTEGER NOT NULL CHECK (max_cursor >= 0)
            );
            ",
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO observation_stats (
                singleton, observation_count, max_append_seq
             )
             SELECT 1, COUNT(*), COALESCE(MAX(append_seq), 0) FROM observations",
            [],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO operational_event_stats (
                data_space_id, event_count, max_cursor
             )
             SELECT data_space_id, COUNT(*), COALESCE(MAX(cursor), 0)
             FROM operational_events
             GROUP BY data_space_id",
            [],
        )?;
        self.backfill_projection_manifest_fields(&transaction)?;
        record_schema_migration(
            &transaction,
            SCHEMA_VERSION_LOCK_SPLIT_SCALARS,
            "append_commit_lock_split_scalars",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn apply_keyset_reads_migration(&self) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        let columns = table_columns(&transaction, "operational_events")?;
        for (column, definition) in [
            ("actor_id", "TEXT"),
            ("causation_id", "TEXT"),
            ("correlation_id", "TEXT"),
        ] {
            if !columns.contains(column) {
                transaction.execute(
                    &format!("ALTER TABLE operational_events ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        transaction.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS projection_visible_blob_refs (
                projection_id TEXT NOT NULL,
                item_key TEXT NOT NULL,
                blob_ref TEXT NOT NULL,
                owner_key TEXT NOT NULL,
                consent_scope TEXT NOT NULL,
                PRIMARY KEY (projection_id, item_key, blob_ref)
            );
            CREATE INDEX IF NOT EXISTS projection_visible_blob_refs_lookup
                ON projection_visible_blob_refs (projection_id, blob_ref);

            CREATE INDEX IF NOT EXISTS projection_materialization_items_owner_order
                ON projection_materialization_items (
                    projection_id, owner_key, sort_key, item_key
                );

            CREATE INDEX IF NOT EXISTS operational_events_correlation_cursor
                ON operational_events(data_space_id, correlation_id, cursor);
            CREATE INDEX IF NOT EXISTS operational_events_stream_cursor
                ON operational_events(data_space_id, stream_id, cursor);
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

            ",
        )?;
        let sync_columns = table_columns(&transaction, "sync_metrics")?;
        if !sync_columns.contains("last_error") {
            transaction.execute("ALTER TABLE sync_metrics ADD COLUMN last_error TEXT", [])?;
        }
        transaction.execute("DROP TRIGGER IF EXISTS operational_events_no_update", [])?;

        let mut statement = transaction.prepare(
            "SELECT cursor, event_json FROM operational_events
             WHERE actor_id IS NULL OR causation_id IS NULL OR correlation_id IS NULL",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for (cursor, event_json) in rows {
            let event: lethe_storage_api::OperationalEvent = serde_json::from_str(&event_json)?;
            transaction.execute(
                "UPDATE operational_events
                 SET actor_id = ?1, causation_id = ?2, correlation_id = ?3
                 WHERE cursor = ?4",
                rusqlite::params![
                    event.actor_id,
                    event.causation_id.map(|id| id.as_str().to_owned()),
                    event.correlation_id,
                    cursor,
                ],
            )?;
        }
        transaction.execute_batch(
            "
            CREATE TRIGGER IF NOT EXISTS operational_events_no_update
            BEFORE UPDATE ON operational_events
            BEGIN
                SELECT RAISE(ABORT, 'operational_events is append-only');
            END;
            ",
        )?;
        record_schema_migration(
            &transaction,
            SCHEMA_VERSION_KEYSET_READS,
            "indexed_keyset_reads",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn apply_privacy_projection_migration(&self) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        let columns = table_columns(&transaction, "projection_visible_blob_refs")?;
        if !columns.contains("subject_key") {
            transaction.execute(
                "ALTER TABLE projection_visible_blob_refs
                 ADD COLUMN subject_key TEXT NOT NULL DEFAULT ''",
                [],
            )?;
            transaction.execute(
                "UPDATE projection_visible_blob_refs
                 SET subject_key = item_key
                 WHERE subject_key = ''",
                [],
            )?;
        }
        transaction.execute_batch(
            "CREATE INDEX IF NOT EXISTS projection_visible_blob_refs_subject_lookup
             ON projection_visible_blob_refs (projection_id, subject_key, blob_ref);",
        )?;
        record_schema_migration(
            &transaction,
            SCHEMA_VERSION_PRIVACY_PROJECTION,
            "privacy_projection_visibility",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn apply_cutover_bridge_migration(&self) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS identity_bridge_candidates (
                v2_identity_key TEXT NOT NULL,
                observation_id TEXT NOT NULL,
                source_instance_id TEXT NOT NULL,
                append_seq INTEGER NOT NULL CHECK (append_seq > 0),
                canonical_json TEXT NOT NULL,
                canonical_json_sha256 TEXT NOT NULL CHECK (length(canonical_json_sha256) = 64),
                PRIMARY KEY (v2_identity_key, observation_id)
            );

            CREATE TABLE IF NOT EXISTS identity_bridge_gaps (
                append_seq INTEGER PRIMARY KEY CHECK (append_seq > 0),
                observation_id TEXT NOT NULL,
                source_instance_id TEXT,
                reason TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS identity_bridge_watermark (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                last_append_seq INTEGER NOT NULL CHECK (last_append_seq >= 0)
            );

            CREATE TABLE IF NOT EXISTS cutover_transition_log (
                event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                source_instance_id TEXT NOT NULL CHECK (length(trim(source_instance_id)) > 0),
                from_phase TEXT NOT NULL,
                to_phase TEXT NOT NULL,
                authority TEXT NOT NULL CHECK (length(trim(authority)) > 0),
                reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
                generation INTEGER NOT NULL CHECK (generation > 0),
                fence_append_seq INTEGER,
                first_v2_append_seq INTEGER,
                committed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cutover_credentials (
                source_instance_id TEXT NOT NULL,
                api_version TEXT NOT NULL CHECK (api_version IN ('v1', 'v2')),
                generation INTEGER NOT NULL CHECK (generation > 0),
                credential_ref TEXT NOT NULL CHECK (length(trim(credential_ref)) > 0),
                active INTEGER NOT NULL CHECK (active IN (0, 1)),
                issued_at TEXT NOT NULL,
                PRIMARY KEY (source_instance_id, api_version, generation)
            );

            CREATE TABLE IF NOT EXISTS cutover_unit_metrics (
                source_instance_id TEXT PRIMARY KEY,
                bridge_duplicate_hits INTEGER NOT NULL CHECK (bridge_duplicate_hits >= 0),
                stale_v1_rejections INTEGER NOT NULL CHECK (stale_v1_rejections >= 0),
                v2_ingested INTEGER NOT NULL CHECK (v2_ingested >= 0),
                updated_at TEXT NOT NULL
            );

            INSERT OR IGNORE INTO identity_bridge_watermark (singleton, last_append_seq)
            VALUES (1, 0);

            CREATE INDEX IF NOT EXISTS identity_bridge_candidates_key_append
                ON identity_bridge_candidates(v2_identity_key, append_seq, observation_id);
            CREATE INDEX IF NOT EXISTS identity_bridge_candidates_source_append
                ON identity_bridge_candidates(source_instance_id, append_seq, observation_id);
            CREATE INDEX IF NOT EXISTS identity_bridge_gaps_source_append
                ON identity_bridge_gaps(source_instance_id, append_seq, observation_id);
            CREATE INDEX IF NOT EXISTS cutover_transition_unit_seq
                ON cutover_transition_log(source_instance_id, event_seq);
            CREATE INDEX IF NOT EXISTS cutover_credentials_active
                ON cutover_credentials(source_instance_id, api_version, active, generation);

            CREATE TRIGGER IF NOT EXISTS cutover_transition_log_no_update
            BEFORE UPDATE ON cutover_transition_log
            BEGIN
                SELECT RAISE(ABORT, 'cutover_transition_log is append-only');
            END;

            CREATE TRIGGER IF NOT EXISTS cutover_transition_log_no_delete
            BEFORE DELETE ON cutover_transition_log
            BEGIN
                SELECT RAISE(ABORT, 'cutover_transition_log is append-only');
            END;
            ",
        )?;
        record_schema_migration(
            &transaction,
            SCHEMA_VERSION_CUTOVER_BRIDGE,
            "v1_v2_cutover_bridge",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn apply_reconsent_privacy_index_migration(&self) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS observation_privacy_keys (
                privacy_key TEXT NOT NULL,
                observation_id TEXT NOT NULL,
                append_seq INTEGER NOT NULL,
                PRIMARY KEY (privacy_key, observation_id)
            );
            CREATE INDEX IF NOT EXISTS observation_privacy_keys_append
                ON observation_privacy_keys (privacy_key, append_seq);
            ",
        )?;
        let mut cursor = 0_u64;
        loop {
            let mut statement = transaction.prepare(
                "SELECT id, append_seq, observation_json
                 FROM observations
                 WHERE append_seq > ?1
                 ORDER BY append_seq
                 LIMIT ?2",
            )?;
            let page = statement
                .query_map(
                    rusqlite::params![cursor, OBSERVATION_SCHEMA_BACKFILL_BATCH_SIZE],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            let Some((_, last_append_seq, _)) = page.last() else {
                break;
            };
            let next_cursor = *last_append_seq;
            for (observation_id, append_seq, json) in page {
                let observation: Observation = serde_json::from_str(&json)?;
                if observation.id.as_str() != observation_id {
                    return Err(PersistenceError::SchemaInvariant(format!(
                        "observation {} disagrees with stored payload {}",
                        observation_id,
                        observation.id.as_str()
                    )));
                }
                for privacy_key in lethe_core::domain::observation_privacy_keys(&observation) {
                    transaction.execute(
                        "INSERT OR IGNORE INTO observation_privacy_keys (
                            privacy_key, observation_id, append_seq
                         ) VALUES (?1, ?2, ?3)",
                        rusqlite::params![privacy_key, observation_id, append_seq],
                    )?;
                }
            }
            cursor = next_cursor;
        }
        record_schema_migration(
            &transaction,
            SCHEMA_VERSION_RECONSENT_PRIVACY_INDEX,
            "reconsent_privacy_reverse_index",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn backfill_projection_manifest_fields(
        &self,
        transaction: &rusqlite::Transaction<'_>,
    ) -> Result<(), PersistenceError> {
        let mut statement = transaction.prepare(
            "SELECT projection_id, records_json
             FROM projection_materializations
             WHERE NOT EXISTS (
                 SELECT 1 FROM projection_manifest_fields fields
                 WHERE fields.projection_id = projection_materializations.projection_id
             )",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let pending = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        for (projection_id, records_json) in pending {
            let value: serde_json::Value = serde_json::from_str(&records_json)?;
            let object = value.as_object().ok_or_else(|| {
                PersistenceError::SchemaInvariant(format!(
                    "projection {projection_id} manifest must be a JSON object"
                ))
            })?;
            for (field_key, field_value) in object {
                transaction.execute(
                    "INSERT OR IGNORE INTO projection_manifest_fields (
                        projection_id, field_key, value_json
                     ) VALUES (?1, ?2, ?3)",
                    params![
                        projection_id,
                        field_key,
                        serde_json::to_string(field_value)?
                    ],
                )?;
            }
        }
        Ok(())
    }

    fn require_schema_migration_name(
        &self,
        version: i64,
        expected_name: &str,
    ) -> Result<(), PersistenceError> {
        let actual_name = self.conn.query_row(
            "SELECT name FROM schema_migrations WHERE version = ?1",
            [version],
            |row| row.get::<_, String>(0),
        )?;
        if actual_name != expected_name {
            return Err(PersistenceError::SchemaInvariant(format!(
                "schema migration v{version} is named {actual_name:?}, expected {expected_name:?}"
            )));
        }
        Ok(())
    }

    fn require_schema_object(
        &self,
        object_type: &str,
        object_name: &str,
    ) -> Result<(), PersistenceError> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2",
                params![object_type, object_name],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(PersistenceError::SchemaInvariant(format!(
                "schema migration object is missing: {object_type} {object_name}"
            )));
        }
        Ok(())
    }

    fn require_lock_split_schema_objects(&self) -> Result<(), PersistenceError> {
        for (object_type, object_name) in [
            ("table", "observation_stats"),
            ("table", "projection_manifest_fields"),
            ("index", "audit_events_timestamp_id"),
            ("table", "operational_event_stats"),
        ] {
            self.require_schema_object(object_type, object_name)?;
        }
        Ok(())
    }

    fn require_keyset_read_schema_objects(&self) -> Result<(), PersistenceError> {
        for (object_type, object_name) in [
            ("index", "operational_events_correlation_cursor"),
            ("index", "operational_events_stream_cursor"),
            ("index", "operational_events_causation_cursor"),
            ("index", "operational_events_type_cursor"),
            ("index", "operational_events_actor_cursor"),
            ("index", "operational_events_occurred_cursor"),
            ("index", "operational_events_stream_occurred_cursor"),
            ("table", "projection_visible_blob_refs"),
            ("index", "projection_visible_blob_refs_lookup"),
        ] {
            self.require_schema_object(object_type, object_name)?;
        }
        let columns = self
            .conn
            .prepare("PRAGMA table_info(operational_events)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        for column in ["actor_id", "causation_id", "correlation_id"] {
            if !columns.contains(column) {
                return Err(PersistenceError::SchemaInvariant(format!(
                    "operational_events column is missing: {column}"
                )));
            }
        }
        let sync_columns = self
            .conn
            .prepare("PRAGMA table_info(sync_metrics)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        if !sync_columns.contains("last_error") {
            return Err(PersistenceError::SchemaInvariant(
                "sync_metrics column is missing: last_error".to_owned(),
            ));
        }
        Ok(())
    }

    fn require_privacy_projection_schema_objects(&self) -> Result<(), PersistenceError> {
        self.require_schema_object("index", "projection_visible_blob_refs_subject_lookup")?;
        let columns = self
            .conn
            .prepare("PRAGMA table_info(projection_visible_blob_refs)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        if !columns.contains("subject_key") {
            return Err(PersistenceError::SchemaInvariant(
                "projection_visible_blob_refs column is missing: subject_key".to_owned(),
            ));
        }
        Ok(())
    }

    fn require_cutover_bridge_schema_objects(&self) -> Result<(), PersistenceError> {
        for (object_type, object_name) in [
            ("table", "identity_bridge_candidates"),
            ("table", "identity_bridge_gaps"),
            ("table", "identity_bridge_watermark"),
            ("table", "cutover_transition_log"),
            ("table", "cutover_credentials"),
            ("table", "cutover_unit_metrics"),
            ("index", "identity_bridge_candidates_key_append"),
            ("index", "identity_bridge_candidates_source_append"),
            ("index", "identity_bridge_gaps_source_append"),
            ("index", "cutover_transition_unit_seq"),
            ("index", "cutover_credentials_active"),
            ("trigger", "cutover_transition_log_no_update"),
            ("trigger", "cutover_transition_log_no_delete"),
        ] {
            self.require_schema_object(object_type, object_name)?;
        }
        Ok(())
    }

    fn require_reconsent_privacy_index_schema_objects(&self) -> Result<(), PersistenceError> {
        for (object_type, object_name) in [
            ("table", "observation_privacy_keys"),
            ("index", "observation_privacy_keys_append"),
        ] {
            self.require_schema_object(object_type, object_name)?;
        }
        Ok(())
    }

    fn migrate_existing_schema(&self) -> Result<(), PersistenceError> {
        let current_version_recorded =
            self.schema_migration_recorded(SCHEMA_VERSION_KEYSET_READS)?;
        let mut columns = self.observation_columns()?;

        let mut route_backfill_required = false;
        if !columns.contains("leaf_id") {
            self.conn
                .execute("ALTER TABLE observations ADD COLUMN leaf_id TEXT", [])?;
            columns.insert("leaf_id".to_owned());
            route_backfill_required = true;
        }
        if !columns.contains("routing_key") {
            self.conn
                .execute("ALTER TABLE observations ADD COLUMN routing_key TEXT", [])?;
            columns.insert("routing_key".to_owned());
            route_backfill_required = true;
        }
        if route_backfill_required {
            self.backfill_observation_routing_columns()?;
        }

        let mut canonical_digest_backfill_required = !current_version_recorded;
        if !columns.contains("canonical_json_sha256") {
            if !columns.contains(CANONICAL_JSON_META_KEY) {
                return Err(PersistenceError::SchemaInvariant(
                    "observations table lacks canonical_json_sha256 and legacy canonical_json; cannot migrate duplicate-detection schema"
                        .to_owned(),
                ));
            }
            self.conn.execute(
                "ALTER TABLE observations ADD COLUMN canonical_json_sha256 TEXT",
                [],
            )?;
            columns.insert("canonical_json_sha256".to_owned());
            canonical_digest_backfill_required = true;
        }
        if canonical_digest_backfill_required && columns.contains(CANONICAL_JSON_META_KEY) {
            self.backfill_canonical_json_sha256_from_legacy_column()?;
        }
        if columns.contains(CANONICAL_JSON_META_KEY) {
            self.rebuild_observations_with_current_columns()?;
            columns = self.observation_columns()?;
        }

        self.require_observation_columns(&columns)
    }

    fn schema_migration_recorded(&self, version: i64) -> Result<bool, PersistenceError> {
        self.conn
            .query_row(
                "SELECT 1 FROM schema_migrations WHERE version = ?1",
                [version],
                |_| Ok(()),
            )
            .optional()
            .map(|row| row.is_some())
            .map_err(PersistenceError::from)
    }

    fn observation_columns(&self) -> Result<HashSet<String>, PersistenceError> {
        let mut statement = self.conn.prepare("PRAGMA table_info(observations)")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
        let mut columns = HashSet::new();
        for row in rows {
            columns.insert(row?);
        }
        Ok(columns)
    }

    fn require_observation_columns(
        &self,
        columns: &HashSet<String>,
    ) -> Result<(), PersistenceError> {
        let missing = [
            "append_seq",
            "id",
            "leaf_id",
            "routing_key",
            "identity_key",
            "canonical_json_sha256",
            "recorded_at",
            "observation_json",
        ]
        .into_iter()
        .filter(|column| !columns.contains(*column))
        .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        Err(PersistenceError::SchemaInvariant(format!(
            "observations table is missing current schema columns: {}",
            missing.join(", ")
        )))
    }

    fn backfill_observation_routing_columns(&self) -> Result<(), PersistenceError> {
        let root_leaf_id = self.root_leaf_id()?;
        loop {
            let transaction = self.conn.unchecked_transaction()?;
            let rows = {
                let mut statement = transaction.prepare(
                    "SELECT append_seq, observation_json
                     FROM observations
                     WHERE leaf_id IS NULL OR routing_key IS NULL
                     ORDER BY append_seq
                     LIMIT ?1",
                )?;
                let mapped = statement
                    .query_map([OBSERVATION_SCHEMA_BACKFILL_BATCH_SIZE], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })?;
                mapped.collect::<Result<Vec<_>, _>>()?
            };
            if rows.is_empty() {
                transaction.commit()?;
                break;
            }

            for (append_seq, observation_json) in rows {
                let observation: Observation = serde_json::from_str(&observation_json)?;
                let routing_key =
                    routing_key_from_observation_for_order(self.routing_key_order, &observation)
                        .map_err(|err| PersistenceError::SchemaInvariant(err.to_string()))?;
                transaction.execute(
                    "UPDATE observations
                     SET leaf_id = ?1, routing_key = ?2
                     WHERE append_seq = ?3",
                    params![root_leaf_id.as_str(), routing_key.encoded(), append_seq],
                )?;
            }
            transaction.commit()?;
        }
        Ok(())
    }

    fn backfill_canonical_json_sha256_from_legacy_column(&self) -> Result<(), PersistenceError> {
        loop {
            let transaction = self.conn.unchecked_transaction()?;
            let rows = {
                let mut statement = transaction.prepare(
                    "SELECT append_seq, canonical_json, observation_json
                     FROM observations
                     WHERE canonical_json_sha256 IS NULL
                     ORDER BY append_seq
                     LIMIT ?1",
                )?;
                let mapped =
                    statement.query_map([OBSERVATION_SCHEMA_BACKFILL_BATCH_SIZE], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?;
                mapped.collect::<Result<Vec<_>, _>>()?
            };
            if rows.is_empty() {
                transaction.commit()?;
                break;
            }

            for (append_seq, canonical_json, observation_json) in rows {
                let metadata_canonical_json =
                    canonical_json_from_observation_json(&observation_json)?;
                if metadata_canonical_json != canonical_json {
                    return Err(PersistenceError::SchemaInvariant(format!(
                        "legacy observations.canonical_json differs from observation.meta.canonical_json at append_seq {append_seq}"
                    )));
                }
                transaction.execute(
                    "UPDATE observations
                     SET canonical_json_sha256 = ?1
                     WHERE append_seq = ?2 AND canonical_json_sha256 IS NULL",
                    params![canonical_json_sha256(&canonical_json), append_seq],
                )?;
            }
            transaction.commit()?;
        }
        Ok(())
    }

    fn rebuild_observations_with_current_columns(&self) -> Result<(), PersistenceError> {
        let transaction = self.conn.unchecked_transaction()?;
        transaction.execute_batch(
            "
            DROP TABLE IF EXISTS observations_rebuild;
            CREATE TABLE observations_rebuild (
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
            INSERT INTO observations_rebuild (
                append_seq,
                id,
                leaf_id,
                routing_key,
                identity_key,
                canonical_json_sha256,
                recorded_at,
                observation_json
            )
            SELECT
                append_seq,
                id,
                leaf_id,
                routing_key,
                identity_key,
                canonical_json_sha256,
                recorded_at,
                observation_json
            FROM observations
            ORDER BY append_seq;
            DROP TABLE observations;
            ALTER TABLE observations_rebuild RENAME TO observations;
            ",
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn backfill_global_identity_registry(
        &self,
        transaction: &rusqlite::Transaction<'_>,
    ) -> Result<(), PersistenceError> {
        transaction.execute(
            "INSERT INTO observation_identity_registry (
                identity_key, observation_id, canonical_json_sha256
             )
             SELECT observation.identity_key, observation.id, observation.canonical_json_sha256
             FROM observations observation
             WHERE observation.append_seq = (
                 SELECT MIN(candidate.append_seq)
                 FROM observations candidate
                 WHERE candidate.identity_key = observation.identity_key
             )
             ON CONFLICT(identity_key) DO NOTHING",
            [],
        )?;
        Ok(())
    }

    fn root_leaf_id(&self) -> Result<String, PersistenceError> {
        self.conn
            .query_row(
                "SELECT leaf_id
                 FROM partition_log
                 WHERE event_type = ?1",
                [PARTITION_EVENT_INITIALIZE],
                |row| row.get(0),
            )
            .map_err(PersistenceError::from)
    }

    pub(super) fn ensure_partition_initialize(&self) -> Result<(), PersistenceError> {
        let expected_routing = routing_keyspec_json_for_order(self.routing_key_order)?;
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

fn record_schema_migration(
    transaction: &rusqlite::Transaction<'_>,
    version: i64,
    name: &str,
) -> Result<(), PersistenceError> {
    transaction.execute(
        "INSERT INTO schema_migrations (version, name, applied_at)
         VALUES (?1, ?2, ?3)",
        params![version, name, chrono::Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn table_columns(
    transaction: &rusqlite::Transaction<'_>,
    table: &str,
) -> Result<std::collections::BTreeSet<String>, PersistenceError> {
    let mut statement = transaction.prepare(&format!("PRAGMA table_info({table})"))?;
    statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
        .map_err(PersistenceError::from)
}
