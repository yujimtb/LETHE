use chrono::{DateTime, Utc};
use lethe_core::domain::DataSpaceId;
use lethe_storage_api::{
    AuditEventCursor, PersistedSyncState, RuntimeStateStore, SyncMetricRecord,
};
use lethe_storage_postgres::PostgresPersistence;
use postgres::{Client, NoTls};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let [_, dsn, schema, expected_role, data_space_id, read_pool_size] = arguments.as_slice()
    else {
        return Err(
            "usage: runtime_conformance <dsn> <schema> <expected-role> <data-space-id> <read-pool-size>"
                .to_owned(),
        );
    };
    let read_pool_size = read_pool_size
        .parse::<usize>()
        .map_err(|error| format!("read-pool-size must be an unsigned integer: {error}"))?;
    let store = PostgresPersistence::connect_database_only_for_tests(
        DataSpaceId::new(data_space_id),
        dsn,
        schema,
        expected_role,
        read_pool_size,
    )
    .map_err(|error| error.to_string())?;

    verify_runtime_state(&store).map_err(|error| format!("runtime state: {error}"))?;
    verify_audit_keyset(&store).map_err(|error| format!("audit keyset: {error}"))?;
    verify_sync_state(&store).map_err(|error| format!("sync state: {error}"))?;
    verify_retention(&store, dsn, schema).map_err(|error| format!("retention: {error}"))?;
    verify_deep_check_corruption_probe(&store, dsn, schema)
        .map_err(|error| format!("deep check: {error}"))?;
    println!("runtime_conformance=passed");
    Ok(())
}

fn verify_runtime_state(store: &PostgresPersistence) -> Result<(), String> {
    if store
        .get_state("runtime:missing")
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("missing runtime state returned a value".to_owned());
    }
    store
        .set_state("runtime:cursor", "")
        .map_err(|error| error.to_string())?;
    store
        .set_state("runtime:cursor", "42")
        .map_err(|error| error.to_string())?;
    if store
        .get_state("runtime:cursor")
        .map_err(|error| error.to_string())?
        .as_deref()
        != Some("42")
    {
        return Err("runtime state upsert did not retain the latest value".to_owned());
    }
    if store.set_state(" ", "value").is_ok() {
        return Err("blank runtime state key was accepted".to_owned());
    }
    Ok(())
}

fn verify_audit_keyset(store: &PostgresPersistence) -> Result<(), String> {
    for (id, timestamp) in [
        ("audit:runtime:1", "2026-07-27T00:00:00Z"),
        ("audit:runtime:2", "2026-07-27T00:00:00Z"),
        ("audit:runtime:3", "2026-07-27T00:00:01Z"),
    ] {
        store
            .record_audit_event(
                id,
                timestamp,
                "actor:runtime-conformance",
                "{\"event\":\"runtime\"}",
            )
            .map_err(|error| error.to_string())?;
    }
    let first = store
        .audit_event_page(None, 2)
        .map_err(|error| error.to_string())?;
    if first
        .iter()
        .map(|event| event.id.as_str())
        .collect::<Vec<_>>()
        != vec!["audit:runtime:1", "audit:runtime:2"]
    {
        return Err("first audit page is not timestamp/id ordered".to_owned());
    }
    let second = store
        .audit_event_page(
            Some(&AuditEventCursor {
                timestamp: first[1].timestamp.clone(),
                id: first[1].id.clone(),
            }),
            2,
        )
        .map_err(|error| error.to_string())?;
    if second
        .iter()
        .map(|event| event.id.as_str())
        .collect::<Vec<_>>()
        != vec!["audit:runtime:3"]
    {
        return Err("audit keyset cursor skipped or duplicated a boundary row".to_owned());
    }
    if store
        .record_audit_event(
            "audit:runtime:invalid",
            "not-a-time",
            "actor:runtime-conformance",
            "{}",
        )
        .is_ok()
        || store
            .record_audit_event(
                "audit:runtime:invalid-json",
                "2026-07-27T00:00:02Z",
                "actor:runtime-conformance",
                "{",
            )
            .is_ok()
    {
        return Err("invalid audit input was accepted".to_owned());
    }
    Ok(())
}

fn verify_sync_state(store: &PostgresPersistence) -> Result<(), String> {
    let state = PersistedSyncState {
        metrics: SyncMetricRecord {
            fetched: 10,
            ingested: 8,
            skipped: 1,
            failed: 1,
            quarantined: 2,
            latency_ms: 345,
        },
        completed_at: timestamp("2026-07-27T01:02:03.456Z"),
        error: Some("fixture error".to_owned()),
    };
    store
        .record_sync_state("source:runtime", &state)
        .map_err(|error| error.to_string())?;
    if store
        .load_sync_state("source:runtime")
        .map_err(|error| error.to_string())?
        != Some(state)
    {
        return Err("persisted sync state did not round-trip".to_owned());
    }
    let replacement = SyncMetricRecord {
        fetched: 4,
        ingested: 4,
        ..SyncMetricRecord::default()
    };
    store
        .record_sync_metrics("source:runtime", &replacement)
        .map_err(|error| error.to_string())?;
    if store
        .load_sync_state("source:runtime")
        .map_err(|error| error.to_string())?
        .map(|state| state.metrics)
        != Some(replacement)
    {
        return Err("record_sync_metrics did not update current sync state".to_owned());
    }
    Ok(())
}

fn verify_retention(store: &PostgresPersistence, dsn: &str, schema: &str) -> Result<(), String> {
    store
        .record_audit_event(
            "audit:runtime:old",
            "2000-01-01T00:00:00Z",
            "actor:runtime-conformance",
            "{\"event\":\"old\"}",
        )
        .map_err(|error| error.to_string())?;
    store
        .record_audit_event(
            "audit:runtime:future",
            "2099-01-01T00:00:00Z",
            "actor:runtime-conformance",
            "{\"event\":\"future\"}",
        )
        .map_err(|error| error.to_string())?;
    store
        .record_dead_letter("source:runtime", "current")
        .map_err(|error| error.to_string())?;
    let mut direct = direct_client(dsn, schema)?;
    direct
        .execute(
            "INSERT INTO dead_letters (source, reason, created_at)
             VALUES ('source:runtime', 'old', '2000-01-01T00:00:00Z')",
            &[],
        )
        .map_err(|error| error.to_string())?;
    let deleted = store
        .apply_retention(30)
        .map_err(|error| error.to_string())?;
    if deleted != 2 {
        return Err(format!(
            "retention deleted {deleted} rows instead of the two old rows"
        ));
    }
    let old_audit: bool = direct
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM audit_events WHERE audit_id = 'audit:runtime:old'
             )",
            &[],
        )
        .map_err(|error| error.to_string())?
        .get(0);
    let future_audit: bool = direct
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM audit_events WHERE audit_id = 'audit:runtime:future'
             )",
            &[],
        )
        .map_err(|error| error.to_string())?
        .get(0);
    let current_dead_letter: bool = direct
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM dead_letters WHERE reason = 'current'
             )",
            &[],
        )
        .map_err(|error| error.to_string())?
        .get(0);
    if old_audit || !future_audit || !current_dead_letter {
        return Err("retention removed the wrong audit/dead-letter rows".to_owned());
    }
    if store.apply_retention(0).is_ok() {
        return Err("zero-day retention was accepted".to_owned());
    }
    Ok(())
}

fn verify_deep_check_corruption_probe(
    store: &PostgresPersistence,
    dsn: &str,
    schema: &str,
) -> Result<(), String> {
    store
        .deep_check_database_only_for_tests()
        .map_err(|error| error.to_string())?;
    let mut direct = direct_client(dsn, schema)?;
    direct
        .execute(
            "UPDATE observation_leaves
             SET observation_count = observation_count + 1
             WHERE parent_leaf_id IS NULL",
            &[],
        )
        .map_err(|error| error.to_string())?;
    if store.deep_check_database_only_for_tests().is_ok() {
        return Err("deep check did not detect a corrupted leaf count".to_owned());
    }
    direct
        .execute(
            "UPDATE observation_leaves
             SET observation_count = (
                SELECT COUNT(*) FROM observations
                WHERE observations.leaf_id = observation_leaves.leaf_id
             )",
            &[],
        )
        .map_err(|error| error.to_string())?;
    store
        .deep_check_database_only_for_tests()
        .map_err(|error| error.to_string())
}

fn direct_client(dsn: &str, schema: &str) -> Result<Client, String> {
    let mut client = Client::connect(dsn, NoTls).map_err(|error| error.to_string())?;
    if !schema
        .chars()
        .all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err("schema contains unsafe identifier characters".to_owned());
    }
    client
        .batch_execute(&format!("SET search_path TO \"{schema}\", pg_catalog"))
        .map_err(|error| error.to_string())?;
    Ok(client)
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("fixed conformance timestamp must parse")
        .with_timezone(&Utc)
}
