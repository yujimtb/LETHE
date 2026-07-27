CREATE TABLE general_storage_pin (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    data_space_id TEXT NOT NULL CHECK (length(btrim(data_space_id)) > 0),
    database_role TEXT NOT NULL CHECK (length(btrim(database_role)) > 0)
);

CREATE TABLE observation_leaves (
    leaf_id TEXT PRIMARY KEY CHECK (length(btrim(leaf_id)) > 0),
    parent_leaf_id TEXT REFERENCES observation_leaves(leaf_id),
    child_side SMALLINT CHECK (child_side IN (0, 1)),
    split_bit_index INTEGER CHECK (split_bit_index >= 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    observation_count BIGINT NOT NULL DEFAULT 0 CHECK (observation_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (parent_leaf_id, child_side),
    CHECK (
        (parent_leaf_id IS NULL AND child_side IS NULL)
        OR (parent_leaf_id IS NOT NULL AND child_side IS NOT NULL)
    ),
    CHECK (
        (active AND split_bit_index IS NULL)
        OR (NOT active AND split_bit_index IS NOT NULL)
    )
);

INSERT INTO observation_leaves (leaf_id)
VALUES ('lake:00000000-0000-7000-8000-000000000000');

CREATE TABLE observations (
    append_seq BIGSERIAL PRIMARY KEY,
    observation_id TEXT NOT NULL UNIQUE CHECK (length(btrim(observation_id)) > 0),
    identity_key TEXT NOT NULL UNIQUE CHECK (length(btrim(identity_key)) > 0),
    canonical_json TEXT NOT NULL,
    routing_key TEXT NOT NULL CHECK (length(routing_key) > 0),
    leaf_id TEXT NOT NULL REFERENCES observation_leaves(leaf_id),
    observed_at TEXT NOT NULL CHECK (length(btrim(observed_at)) > 0),
    observation_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (observation_id, append_seq)
);

CREATE INDEX observations_leaf_page
    ON observations (leaf_id, append_seq);

CREATE TABLE observation_privacy_keys (
    privacy_key TEXT NOT NULL CHECK (length(btrim(privacy_key)) > 0),
    append_seq BIGINT NOT NULL REFERENCES observations(append_seq),
    PRIMARY KEY (privacy_key, append_seq)
);

CREATE TABLE supplementals (
    supplemental_id TEXT PRIMARY KEY CHECK (length(btrim(supplemental_id)) > 0),
    created_at TEXT NOT NULL CHECK (length(btrim(created_at)) > 0),
    supplemental_json JSONB NOT NULL
);

CREATE INDEX supplementals_page
    ON supplementals (created_at, supplemental_id);

CREATE TABLE blob_objects (
    blob_ref TEXT PRIMARY KEY
        CHECK (blob_ref ~ '^blob:sha256:[0-9a-f]{64}$'),
    object_key TEXT NOT NULL UNIQUE CHECK (length(btrim(object_key)) > 0),
    byte_count BIGINT NOT NULL CHECK (byte_count >= 0),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE blob_orphan_scan_state (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    scan_generation BIGINT NOT NULL DEFAULT 0 CHECK (scan_generation >= 0)
);

INSERT INTO blob_orphan_scan_state (singleton) VALUES (TRUE);

CREATE TABLE blob_orphan_candidates (
    blob_ref TEXT PRIMARY KEY
        CHECK (blob_ref ~ '^blob:sha256:[0-9a-f]{64}$'),
    object_key TEXT NOT NULL UNIQUE CHECK (length(btrim(object_key)) > 0),
    byte_count BIGINT NOT NULL CHECK (byte_count >= 0),
    first_unreferenced_at TIMESTAMPTZ NOT NULL,
    last_scan_generation BIGINT NOT NULL CHECK (last_scan_generation > 0),
    consecutive_scans INTEGER NOT NULL CHECK (consecutive_scans > 0)
);

CREATE TABLE projection_materializations (
    projection_id TEXT NOT NULL CHECK (length(btrim(projection_id)) > 0),
    keyspec_version TEXT NOT NULL CHECK (length(btrim(keyspec_version)) > 0),
    generation BIGINT NOT NULL CHECK (generation > 0),
    manifest_json JSONB NOT NULL,
    records_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_id, keyspec_version, generation)
);

CREATE TABLE projection_materialization_heads (
    projection_id TEXT NOT NULL,
    keyspec_version TEXT NOT NULL,
    generation BIGINT NOT NULL,
    PRIMARY KEY (projection_id, keyspec_version),
    FOREIGN KEY (projection_id, keyspec_version, generation)
        REFERENCES projection_materializations (
            projection_id, keyspec_version, generation
        )
);

CREATE TABLE projection_materialization_items (
    projection_id TEXT NOT NULL,
    keyspec_version TEXT NOT NULL,
    generation BIGINT NOT NULL,
    item_key TEXT NOT NULL CHECK (length(btrim(item_key)) > 0),
    owner_key TEXT NOT NULL CHECK (length(btrim(owner_key)) > 0),
    sort_key TEXT NOT NULL CHECK (length(btrim(sort_key)) > 0),
    item_json JSONB NOT NULL,
    PRIMARY KEY (
        projection_id, keyspec_version, generation, item_key
    ),
    FOREIGN KEY (projection_id, keyspec_version, generation)
        REFERENCES projection_materializations (
            projection_id, keyspec_version, generation
        ) ON DELETE CASCADE
);

CREATE INDEX projection_items_owner
    ON projection_materialization_items (
        projection_id, keyspec_version, generation, owner_key, sort_key, item_key
    );

CREATE TABLE projection_visible_blob_refs (
    projection_id TEXT NOT NULL,
    keyspec_version TEXT NOT NULL,
    generation BIGINT NOT NULL,
    blob_ref TEXT NOT NULL REFERENCES blob_objects(blob_ref),
    PRIMARY KEY (
        projection_id, keyspec_version, generation, blob_ref
    ),
    FOREIGN KEY (projection_id, keyspec_version, generation)
        REFERENCES projection_materializations (
            projection_id, keyspec_version, generation
        ) ON DELETE CASCADE
);

CREATE TABLE retired_projection_materializations (
    projection_id TEXT NOT NULL,
    keyspec_version TEXT NOT NULL,
    generation BIGINT NOT NULL,
    retired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_id, keyspec_version, generation)
);

CREATE TABLE projection_leaf_watermarks (
    projection_id TEXT NOT NULL,
    keyspec_version TEXT NOT NULL,
    leaf_id TEXT NOT NULL REFERENCES observation_leaves(leaf_id),
    append_seq BIGINT NOT NULL CHECK (append_seq >= 0),
    status TEXT NOT NULL CHECK (length(btrim(status)) > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (projection_id, keyspec_version, leaf_id)
);

CREATE TABLE runtime_state (
    state_key TEXT PRIMARY KEY CHECK (length(btrim(state_key)) > 0),
    state_value TEXT NOT NULL
);

CREATE TABLE sync_state (
    source TEXT PRIMARY KEY CHECK (length(btrim(source)) > 0),
    state_json JSONB NOT NULL
);

CREATE TABLE dead_letters (
    dead_letter_seq BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL CHECK (length(btrim(source)) > 0),
    reason TEXT NOT NULL CHECK (length(btrim(reason)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE audit_events (
    audit_seq BIGSERIAL PRIMARY KEY,
    audit_id TEXT NOT NULL UNIQUE CHECK (length(btrim(audit_id)) > 0),
    timestamp_text TEXT NOT NULL CHECK (length(btrim(timestamp_text)) > 0),
    actor TEXT NOT NULL CHECK (length(btrim(actor)) > 0),
    event_json TEXT NOT NULL
);

CREATE INDEX audit_events_keyset
    ON audit_events (timestamp_text, audit_id);

CREATE TABLE sync_metrics (
    metric_seq BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL CHECK (length(btrim(source)) > 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metrics_json JSONB NOT NULL
);

CREATE TABLE slack_thread_catalog_state (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    discovery_high_water BIGINT NOT NULL DEFAULT 0
        CHECK (discovery_high_water >= 0),
    poll_generation BIGINT NOT NULL DEFAULT 0 CHECK (poll_generation >= 0)
);

INSERT INTO slack_thread_catalog_state (singleton) VALUES (TRUE);

CREATE TABLE slack_thread_catalog (
    source_instance_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    thread_ts TEXT NOT NULL,
    discovered_append_seq BIGINT NOT NULL REFERENCES observations(append_seq),
    reply_cursor TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    next_poll_generation BIGINT NOT NULL CHECK (next_poll_generation >= 0),
    PRIMARY KEY (source_instance_id, channel_id, thread_ts)
);

CREATE INDEX slack_threads_poll
    ON slack_thread_catalog (
        source_instance_id, channel_id, active, next_poll_generation, thread_ts
    );

CREATE TABLE cutover_states (
    source_instance_id TEXT PRIMARY KEY,
    authority TEXT NOT NULL CHECK (length(btrim(authority)) > 0),
    phase TEXT NOT NULL
        CHECK (phase IN ('v1_active', 'draining', 'v2_active', 'v2_committed')),
    generation BIGINT NOT NULL CHECK (generation > 0),
    fence_append_seq BIGINT CHECK (fence_append_seq >= 0),
    first_v2_append_seq BIGINT CHECK (first_v2_append_seq > 0),
    v2_ingested BIGINT NOT NULL DEFAULT 0 CHECK (v2_ingested >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE identity_bridge_candidates (
    v2_identity_key TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    source_instance_id TEXT NOT NULL,
    append_seq BIGINT NOT NULL,
    canonical_json TEXT NOT NULL,
    canonical_json_sha256 TEXT NOT NULL
        CHECK (canonical_json_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (v2_identity_key, observation_id),
    FOREIGN KEY (observation_id, append_seq)
        REFERENCES observations(observation_id, append_seq)
);

CREATE TABLE identity_bridge_gaps (
    append_seq BIGINT PRIMARY KEY,
    observation_id TEXT NOT NULL,
    source_instance_id TEXT,
    reason TEXT NOT NULL CHECK (length(btrim(reason)) > 0),
    FOREIGN KEY (observation_id, append_seq)
        REFERENCES observations(observation_id, append_seq)
);

CREATE TABLE identity_bridge_watermark (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton),
    append_seq BIGINT NOT NULL DEFAULT 0 CHECK (append_seq >= 0)
);

INSERT INTO identity_bridge_watermark (singleton) VALUES (TRUE);

CREATE TABLE cutover_transition_log (
    transition_seq BIGSERIAL PRIMARY KEY,
    source_instance_id TEXT NOT NULL,
    authority TEXT NOT NULL,
    reason TEXT NOT NULL,
    from_phase TEXT NOT NULL,
    to_phase TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    fence_append_seq BIGINT,
    first_v2_append_seq BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE cutover_credentials (
    source_instance_id TEXT NOT NULL,
    api_version TEXT NOT NULL CHECK (api_version IN ('v1', 'v2')),
    generation BIGINT NOT NULL CHECK (generation > 0),
    credential_id TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_instance_id, api_version, generation),
    UNIQUE (source_instance_id, credential_id)
);

CREATE TABLE cutover_unit_metrics (
    source_instance_id TEXT PRIMARY KEY,
    bridge_duplicate_hits BIGINT NOT NULL DEFAULT 0
        CHECK (bridge_duplicate_hits >= 0),
    stale_v1_rejections BIGINT NOT NULL DEFAULT 0
        CHECK (stale_v1_rejections >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION reject_general_observation_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'observations is append-only';
END;
$$;

CREATE TRIGGER observations_no_immutable_update
    BEFORE UPDATE OF observation_id, identity_key, canonical_json,
        routing_key, observed_at, observation_json
    ON observations
    FOR EACH ROW EXECUTE FUNCTION reject_general_observation_mutation();

CREATE TRIGGER observations_no_delete
    BEFORE DELETE ON observations
    FOR EACH ROW EXECUTE FUNCTION reject_general_observation_mutation();

CREATE TRIGGER cutover_transition_log_no_update
    BEFORE UPDATE ON cutover_transition_log
    FOR EACH ROW EXECUTE FUNCTION reject_general_observation_mutation();

CREATE TRIGGER cutover_transition_log_no_delete
    BEFORE DELETE ON cutover_transition_log
    FOR EACH ROW EXECUTE FUNCTION reject_general_observation_mutation();
