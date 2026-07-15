use super::*;

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

            CREATE INDEX IF NOT EXISTS observations_leaf_append
                ON observations(leaf_id, append_seq);

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
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
            params![
                CURRENT_SCHEMA_VERSION,
                "canonical_json_sha256",
                chrono::Utc::now().to_rfc3339(),
            ],
        )?;
        self.ensure_partition_initialize()?;
        Ok(())
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
