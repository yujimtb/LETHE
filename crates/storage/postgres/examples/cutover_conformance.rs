use lethe_core::domain::{DataSpaceId, IdempotencyKey, Observation};
use lethe_storage_api::{
    AppendOutcome, AuditEventRecord, CutoverApiVersion, CutoverFixture, CutoverPhase, CutoverStore,
    ObservationStore, StorageError, conformance,
};
use lethe_storage_postgres::PostgresPersistence;
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};

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
            "usage: cutover_conformance <dsn> <schema> <expected-role> <data-space-id> <read-pool-size>"
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

    verify_bridge_and_cutover(&store)?;
    verify_atomic_pages(&store, dsn, schema)?;
    verify_precommit_rollback(&store)?;
    verify_bridge_batch_rollback(&store, dsn, schema)?;
    verify_transition_history_validation(&store, dsn, schema)?;
    println!("cutover_conformance=passed");
    Ok(())
}

fn verify_atomic_pages(store: &PostgresPersistence, dsn: &str, schema: &str) -> Result<(), String> {
    let unit = "unit-a";
    let canonical = serde_json::json!({"body": "atomic new"}).to_string();
    let identity = bridge_identity(unit, "atomic-new", &canonical);
    let observation = bridge_observation(unit, "atomic-new", &canonical, &identity);
    let appended = store
        .append_observations_v2_atomic_page(unit, 2, std::slice::from_ref(&observation), &[])
        .map_err(|error| error.to_string())?;
    if !matches!(appended.as_slice(), [AppendOutcome::Appended(_)]) {
        return Err("atomic PostgreSQL page did not append its new item".to_owned());
    }
    let duplicate = store
        .append_observations_v2_atomic_page(unit, 2, &[observation], &[])
        .map_err(|error| error.to_string())?;
    if !matches!(duplicate.as_slice(), [AppendOutcome::Duplicate(_)]) {
        return Err("atomic PostgreSQL page retry did not converge to duplicate".to_owned());
    }
    let concurrent_canonical = serde_json::json!({"body": "concurrent atomic retry"}).to_string();
    let concurrent_identity = bridge_identity(unit, "atomic-concurrent", &concurrent_canonical);
    let concurrent = bridge_observation(
        unit,
        "atomic-concurrent",
        &concurrent_canonical,
        &concurrent_identity,
    );
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let concurrent_results = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let barrier = std::sync::Arc::clone(&barrier);
            let observation = concurrent.clone();
            handles.push(scope.spawn(move || {
                barrier.wait();
                store.append_observations_v2_atomic_page(unit, 2, &[observation], &[])
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "atomic PostgreSQL retry thread panicked".to_owned())?
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()
    })?;
    let mut concurrent_outcomes = concurrent_results
        .into_iter()
        .map(|outcomes| match outcomes.as_slice() {
            [AppendOutcome::Appended(_)] => Ok("appended"),
            [AppendOutcome::Duplicate(_)] => Ok("duplicate"),
            other => Err(format!(
                "unexpected concurrent atomic PostgreSQL result: {other:?}"
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    concurrent_outcomes.sort_unstable();
    if concurrent_outcomes != ["appended", "duplicate"] {
        return Err(format!(
            "concurrent exact atomic retries did not converge: {concurrent_outcomes:?}"
        ));
    }

    let colliding_identity_canonical = serde_json::json!({"body": "canary"}).to_string();
    let colliding_identity = bridge_identity(unit, "object-1", &colliding_identity_canonical);
    let mismatched_canonical = serde_json::json!({"body": "forced mismatch"}).to_string();
    let mismatch = bridge_observation(
        unit,
        "collision-seed",
        &mismatched_canonical,
        "unit-a:legacy:collision-seed",
    );
    store
        .append_observation(&mismatch)
        .map_err(|error| error.to_string())?;
    let mut direct = direct_client(dsn, schema)?;
    let append_seq: i64 = direct
        .query_one(
            "SELECT append_seq FROM observations WHERE observation_id = $1",
            &[&mismatch.id.as_str()],
        )
        .map_err(|error| error.to_string())?
        .get(0);
    direct
        .execute(
            "INSERT INTO identity_bridge_candidates (
                v2_identity_key, observation_id, source_instance_id, append_seq,
                canonical_json, canonical_json_sha256
             ) VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &colliding_identity,
                &mismatch.id.as_str(),
                &unit,
                &append_seq,
                &mismatched_canonical,
                &hex::encode(Sha256::digest(mismatched_canonical.as_bytes())),
            ],
        )
        .map_err(|error| error.to_string())?;

    let before_collision = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count;
    let new_canonical = serde_json::json!({"body": "must roll back"}).to_string();
    let new_observation = bridge_observation(
        unit,
        "atomic-rollback",
        &new_canonical,
        &bridge_identity(unit, "atomic-rollback", &new_canonical),
    );
    let collision = bridge_observation(
        unit,
        "object-1",
        &colliding_identity_canonical,
        &colliding_identity,
    );
    if !matches!(
        store.append_observations_v2_atomic_page(unit, 2, &[new_observation, collision], &[],),
        Err(StorageError::AtomicPageCollision { index: 1, .. })
    ) {
        return Err("PostgreSQL atomic page collision was not typed".to_owned());
    }
    if store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count
        != before_collision
    {
        return Err("PostgreSQL atomic page collision leaked an append".to_owned());
    }

    let audit = AuditEventRecord {
        id: "audit:postgres-atomic-duplicate".to_owned(),
        timestamp: "2026-07-27T00:00:00Z".to_owned(),
        actor: "actor:test".to_owned(),
        event_json: "{}".to_owned(),
    };
    let audit_failure_canonical = serde_json::json!({"body": "audit failure"}).to_string();
    let audit_failure = bridge_observation(
        unit,
        "atomic-audit-failure",
        &audit_failure_canonical,
        &bridge_identity(unit, "atomic-audit-failure", &audit_failure_canonical),
    );
    if store
        .append_observations_v2_atomic_page(unit, 2, &[audit_failure], &[audit.clone(), audit])
        .is_ok()
    {
        return Err("PostgreSQL duplicate audit failure did not abort atomic page".to_owned());
    }
    if store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count
        != before_collision
    {
        return Err("PostgreSQL audit failure leaked an atomic page append".to_owned());
    }

    let stale_canonical = serde_json::json!({"body": "stale"}).to_string();
    let stale = bridge_observation(
        unit,
        "atomic-stale",
        &stale_canonical,
        &bridge_identity(unit, "atomic-stale", &stale_canonical),
    );
    if !matches!(
        store.append_observations_v2_atomic_page(unit, 1, &[stale], &[]),
        Err(StorageError::CutoverAdmissionDenied(_))
    ) {
        return Err("PostgreSQL atomic page accepted stale generation".to_owned());
    }
    Ok(())
}

fn verify_bridge_and_cutover(store: &PostgresPersistence) -> Result<(), String> {
    let unit = "unit-a";
    let object_id = "object-1";
    let canonical = serde_json::json!({"body": "canary"}).to_string();
    let legacy = bridge_observation(unit, object_id, &canonical, "unit-a:legacy:object-1");
    let outcomes = store
        .append_observations_v1_with_admission(unit, None, std::slice::from_ref(&legacy), &[])
        .map_err(|error| error.to_string())?;
    if !matches!(outcomes.as_slice(), [AppendOutcome::Appended(_)]) {
        return Err("unregistered v1 append did not append".to_owned());
    }
    let bridge = store
        .identity_bridge_apply_batch(16)
        .map_err(|error| error.to_string())?;
    if bridge.read_count != 1 || bridge.candidate_count != 1 || bridge.watermark != 1 {
        return Err("identity bridge batch did not index the legacy observation".to_owned());
    }
    let identity = bridge_identity(unit, object_id, &canonical);
    let resolution = store
        .identity_bridge_resolve(&identity, &canonical)
        .map_err(|error| error.to_string())?;
    if resolution.winner.as_ref() != Some(&legacy.id)
        || resolution.multiplicity != 1
        || resolution.canonical_collision
    {
        return Err("identity bridge resolution did not select the legacy observation".to_owned());
    }

    let registered = store
        .cutover_register(unit, "owner:test", "register")
        .map_err(|error| error.to_string())?;
    if registered.phase != CutoverPhase::V1Active || registered.generation != 1 {
        return Err("cutover registration did not create v1 generation 1".to_owned());
    }
    store
        .cutover_admit(unit, CutoverApiVersion::V1, Some(1))
        .map_err(|error| error.to_string())?;
    let draining = store
        .cutover_begin_drain(unit, "owner:test", "fence")
        .map_err(|error| error.to_string())?;
    if draining.phase != CutoverPhase::Draining || draining.fence_append_seq != Some(1) {
        return Err("cutover drain did not persist the observation fence".to_owned());
    }
    let fixture = CutoverFixture {
        object_id: object_id.to_owned(),
        canonical_json: canonical.clone(),
        expected_identity_key: identity.clone(),
        expected_observation_id: Some(legacy.id.clone()),
    };
    let readiness = store
        .cutover_readiness(unit, Some(&fixture))
        .map_err(|error| error.to_string())?;
    if !readiness.ready {
        return Err(format!(
            "cutover readiness unexpectedly blocked: {readiness:?}"
        ));
    }
    let active = store
        .cutover_activate(unit, "owner:test", "activate", &fixture)
        .map_err(|error| error.to_string())?;
    if active.phase != CutoverPhase::V2Active || active.generation != 2 {
        return Err("cutover activation did not issue v2 generation 2".to_owned());
    }
    if !matches!(
        store.cutover_admit(unit, CutoverApiVersion::V1, Some(1)),
        Err(StorageError::CutoverAdmissionDenied(_))
    ) || !matches!(
        store.cutover_admit(unit, CutoverApiVersion::V1, Some(2)),
        Err(StorageError::CutoverAdmissionDenied(_))
    ) {
        return Err("stale v1 admission was not denied".to_owned());
    }
    store
        .cutover_admit(unit, CutoverApiVersion::V2, Some(2))
        .map_err(|error| error.to_string())?;

    let duplicate = bridge_observation(unit, object_id, &canonical, &identity);
    if !matches!(
        store
            .append_observations_v2_with_bridge(unit, Some(2), &[duplicate], &[])
            .map_err(|error| error.to_string())?
            .as_slice(),
        [AppendOutcome::Duplicate(_)]
    ) {
        return Err("v2 bridge did not resolve the legacy duplicate".to_owned());
    }
    let new_canonical = serde_json::json!({"body": "new"}).to_string();
    let new_identity = bridge_identity(unit, "object-2", &new_canonical);
    let new_observation = bridge_observation(unit, "object-2", &new_canonical, &new_identity);
    if !matches!(
        store
            .append_observations_v2_with_bridge(unit, Some(2), &[new_observation], &[])
            .map_err(|error| error.to_string())?
            .as_slice(),
        [AppendOutcome::Appended(_)]
    ) {
        return Err("first new v2 observation was not appended".to_owned());
    }
    let committed = store
        .cutover_state(unit)
        .map_err(|error| error.to_string())?;
    if committed.phase != CutoverPhase::V2Committed
        || committed.v2_ingested != 1
        || committed.first_v2_append_seq.is_none()
    {
        return Err("first v2 append did not durably commit the cutover".to_owned());
    }
    if !matches!(
        store.cutover_rollback(unit, "owner:test", "unsafe"),
        Err(StorageError::CutoverRollbackRefused(_))
    ) {
        return Err("rollback after v2 ingest was not refused".to_owned());
    }
    let health = store
        .cutover_health(unit)
        .map_err(|error| error.to_string())?;
    if health.bridge_duplicate_hit_count != 1
        || health.stale_v1_rejection_count != 2
        || health.state.v2_ingested != 1
    {
        return Err(format!("cutover health counters are incorrect: {health:?}"));
    }
    let inventory = store
        .cutover_inventory()
        .map_err(|error| error.to_string())?;
    let unit_inventory = inventory
        .iter()
        .find(|item| item.source_instance_id == unit)
        .ok_or_else(|| "cutover inventory omitted unit-a".to_owned())?;
    if unit_inventory.producer_ids != vec!["producer-a"]
        || unit_inventory.credential_ids != vec!["credential-a"]
    {
        return Err("cutover inventory did not use canonical metadata keys".to_owned());
    }
    Ok(())
}

fn verify_precommit_rollback(store: &PostgresPersistence) -> Result<(), String> {
    store
        .cutover_register("unit-b", "owner:test", "register")
        .map_err(|error| error.to_string())?;
    store
        .cutover_begin_drain("unit-b", "owner:test", "fence")
        .map_err(|error| error.to_string())?;
    let rolled_back = store
        .cutover_rollback("unit-b", "owner:test", "pre-commit rollback")
        .map_err(|error| error.to_string())?;
    if rolled_back.phase != CutoverPhase::V1Active || rolled_back.generation != 2 {
        return Err("pre-commit rollback did not issue a new v1 generation".to_owned());
    }
    store
        .cutover_admit("unit-b", CutoverApiVersion::V1, Some(2))
        .map_err(|error| error.to_string())
}

fn verify_bridge_batch_rollback(
    store: &PostgresPersistence,
    dsn: &str,
    schema: &str,
) -> Result<(), String> {
    let before = store
        .identity_bridge_watermark()
        .map_err(|error| error.to_string())?;
    let canonical = serde_json::json!({"body": "failure injection"}).to_string();
    let observation = bridge_observation(
        "unit-failure",
        "object-1",
        &canonical,
        "unit-failure:legacy",
    );
    store
        .append_observation(&observation)
        .map_err(|error| error.to_string())?;
    let mut direct = direct_client(dsn, schema)?;
    direct
        .batch_execute(
            "CREATE FUNCTION fail_bridge_watermark()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 RAISE EXCEPTION 'injected bridge watermark failure';
             END;
             $$;
             CREATE TRIGGER fail_bridge_watermark
             BEFORE UPDATE ON identity_bridge_watermark
             FOR EACH ROW EXECUTE FUNCTION fail_bridge_watermark();",
        )
        .map_err(|error| error.to_string())?;
    if store.identity_bridge_apply_batch(16).is_ok() {
        return Err("injected bridge watermark failure did not abort the batch".to_owned());
    }
    let candidate_visible: bool = direct
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM identity_bridge_candidates
                WHERE observation_id = $1
             )",
            &[&observation.id.as_str()],
        )
        .map_err(|error| error.to_string())?
        .get(0);
    if candidate_visible
        || store
            .identity_bridge_watermark()
            .map_err(|error| error.to_string())?
            != before
    {
        return Err("failed bridge batch leaked candidate or watermark state".to_owned());
    }
    direct
        .batch_execute(
            "DROP TRIGGER fail_bridge_watermark ON identity_bridge_watermark;
             DROP FUNCTION fail_bridge_watermark();",
        )
        .map_err(|error| error.to_string())?;
    let retried = store
        .identity_bridge_apply_batch(16)
        .map_err(|error| error.to_string())?;
    if retried.watermark <= before || retried.candidate_count == 0 {
        return Err("bridge batch did not recover after failure removal".to_owned());
    }
    Ok(())
}

fn verify_transition_history_validation(
    store: &PostgresPersistence,
    dsn: &str,
    schema: &str,
) -> Result<(), String> {
    store
        .cutover_register("unit-corrupt", "owner:test", "register")
        .map_err(|error| error.to_string())?;
    let mut direct = direct_client(dsn, schema)?;
    direct
        .execute(
            "INSERT INTO cutover_transition_log (
                source_instance_id, authority, reason, from_phase,
                to_phase, generation
             ) VALUES (
                'unit-corrupt', 'owner:test', 'invalid direct activation',
                'v1_active', 'v2_active', 2
             )",
            &[],
        )
        .map_err(|error| error.to_string())?;
    if store.cutover_state("unit-corrupt").is_ok() {
        return Err("invalid cutover transition history was accepted".to_owned());
    }
    if direct
        .execute(
            "DELETE FROM cutover_transition_log
             WHERE source_instance_id = 'unit-corrupt'
               AND reason = 'invalid direct activation'",
            &[],
        )
        .is_ok()
    {
        return Err("cutover transition history allowed deletion".to_owned());
    }
    Ok(())
}

fn bridge_observation(
    source_instance: &str,
    object_id: &str,
    canonical_json: &str,
    identity_key: &str,
) -> Observation {
    let mut observation = conformance::sample_observation(identity_key);
    observation.idempotency_key = IdempotencyKey::new(identity_key);
    observation.meta = serde_json::json!({
        "canonical_json": canonical_json,
        "source_instance": source_instance,
        "object_id": object_id,
        "producer_id": "producer-a",
        "credential_id": "credential-a",
        "source_container": "cutover-conformance",
    });
    observation
}

fn bridge_identity(source_instance: &str, object_id: &str, canonical_json: &str) -> String {
    format!(
        "{source_instance}:{object_id}:{}",
        hex::encode(Sha256::digest(canonical_json.as_bytes()))
    )
}

fn direct_client(dsn: &str, schema: &str) -> Result<Client, String> {
    if !schema
        .chars()
        .all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err("schema contains unsafe identifier characters".to_owned());
    }
    let mut client = Client::connect(dsn, NoTls).map_err(|error| error.to_string())?;
    client
        .batch_execute(&format!("SET search_path TO \"{schema}\", pg_catalog"))
        .map_err(|error| error.to_string())?;
    Ok(client)
}
