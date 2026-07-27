use std::sync::{Arc, Barrier};

use lethe_core::domain::{DataSpaceId, IdempotencyKey, Observation};
use lethe_storage_api::{
    AppendOutcome, AuditEventRecord, ObservationStore, RehomeMode, conformance,
};
use lethe_storage_postgres::PostgresPersistence;

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
        return Err("usage: observation_conformance <dsn> <schema> <expected-role> <data-space-id> <read-pool-size>".to_owned());
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

    conformance::observation_store_round_trip(&store);
    verify_collision(&store)?;
    verify_atomic_audit_rollback(&store)?;
    verify_privacy_and_split(&store)?;
    verify_page_and_rehome(&store)?;
    verify_concurrent_idempotency(dsn, schema, expected_role, data_space_id)?;
    println!("observation_conformance=passed");
    Ok(())
}

fn verify_collision(store: &PostgresPersistence) -> Result<(), String> {
    let original = conformance::sample_observation("postgres:collision");
    require_outcome(
        store
            .append_observation(&original)
            .map_err(|error| error.to_string())?,
        "appended",
    )?;
    let mut collision = original.clone();
    collision.id = Observation::new_id();
    collision.meta["canonical_json"] = serde_json::Value::String("{\"changed\":true}".to_owned());
    require_outcome(
        store
            .append_observation(&collision)
            .map_err(|error| error.to_string())?,
        "canonical_collision",
    )
}

fn verify_atomic_audit_rollback(store: &PostgresPersistence) -> Result<(), String> {
    let first = conformance::sample_observation("postgres:audit:first");
    let audit = AuditEventRecord {
        id: "audit:postgres:duplicate".to_owned(),
        timestamp: "2026-07-27T00:00:00Z".to_owned(),
        actor: "test:postgres".to_owned(),
        event_json: "{\"event\":\"append\"}".to_owned(),
    };
    store
        .append_observations_with_audit(std::slice::from_ref(&first), std::slice::from_ref(&audit))
        .map_err(|error| error.to_string())?;

    let rolled_back = conformance::sample_observation("postgres:audit:rollback");
    if store
        .append_observations_with_audit(
            std::slice::from_ref(&rolled_back),
            std::slice::from_ref(&audit),
        )
        .is_ok()
    {
        return Err("duplicate audit id did not fail the transaction".to_owned());
    }
    if store
        .observation_by_id(&rolled_back.id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("observation remained visible after audit rollback".to_owned());
    }
    Ok(())
}

fn verify_privacy_and_split(store: &PostgresPersistence) -> Result<(), String> {
    let privacy = conformance::sample_observation("postgres:privacy");
    store
        .append_observation(&privacy)
        .map_err(|error| error.to_string())?;
    let indexed = store
        .observations_for_privacy_key(privacy.subject.as_str())
        .map_err(|error| error.to_string())?;
    if !indexed
        .iter()
        .any(|stored| stored.observation.id == privacy.id)
    {
        return Err("privacy reverse index omitted appended observation".to_owned());
    }
    if !store
        .split_leaf_if_capacity(2)
        .map_err(|error| error.to_string())?
    {
        return Err("capacity split did not split an eligible leaf".to_owned());
    }
    let positions = store.leaf_positions().map_err(|error| error.to_string())?;
    if positions.len() < 2 {
        return Err("capacity split exposed fewer than two active leaves".to_owned());
    }
    Ok(())
}

fn verify_page_and_rehome(store: &PostgresPersistence) -> Result<(), String> {
    let source = conformance::sample_observation("postgres:rehome:stored");
    store
        .append_observation(&source)
        .map_err(|error| error.to_string())?;
    require_outcome(
        store
            .rehome_observation(&source, RehomeMode::StoredIdentity)
            .map_err(|error| error.to_string())?,
        "duplicate",
    )?;

    let recomputed = conformance::sample_observation("postgres:rehome:input");
    let recomputed_id = recomputed.id.clone();
    require_outcome(
        store
            .rehome_observation(
                &recomputed,
                RehomeMode::RecomputedIdentity {
                    identity_key: IdempotencyKey::new("postgres:rehome:recomputed"),
                    canonical_json: "{\"rehomed\":true}".to_owned(),
                },
            )
            .map_err(|error| error.to_string())?,
        "appended",
    )?;
    if store
        .observation_by_id(&recomputed_id)
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return Err("recomputed rehome was not readable by id".to_owned());
    }

    let page = store
        .observation_page(0, 100)
        .map_err(|error| error.to_string())?;
    if page
        .windows(2)
        .any(|pair| pair[0].append_seq >= pair[1].append_seq)
    {
        return Err("observation page is not strictly append-sequence ordered".to_owned());
    }
    for leaf in store.leaf_positions().map_err(|error| error.to_string())? {
        let leaf_page = store
            .observations_for_leaf_after(&leaf.leaf_id, 0, 100)
            .map_err(|error| error.to_string())?;
        if leaf_page
            .iter()
            .any(|stored| stored.leaf_id != leaf.leaf_id)
        {
            return Err(format!("leaf page {} contained another leaf", leaf.leaf_id));
        }
    }
    Ok(())
}

fn verify_concurrent_idempotency(
    dsn: &str,
    schema: &str,
    expected_role: &str,
    data_space_id: &str,
) -> Result<(), String> {
    let observation = conformance::sample_observation("postgres:concurrent");
    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let worker_observation = observation.clone();
        let worker_barrier = Arc::clone(&barrier);
        let worker_dsn = dsn.to_owned();
        let worker_schema = schema.to_owned();
        let worker_role = expected_role.to_owned();
        let worker_data_space = data_space_id.to_owned();
        handles.push(std::thread::spawn(move || {
            let worker_store = PostgresPersistence::connect_database_only_for_tests(
                DataSpaceId::new(worker_data_space),
                &worker_dsn,
                &worker_schema,
                &worker_role,
                1,
            )?;
            worker_barrier.wait();
            worker_store.append_observation(&worker_observation)
        }));
    }
    let mut appended = 0;
    let mut duplicate = 0;
    for handle in handles {
        let outcome = handle
            .join()
            .map_err(|_| "concurrent append worker panicked".to_owned())?
            .map_err(|error| error.to_string())?;
        match outcome {
            AppendOutcome::Appended(_) => appended += 1,
            AppendOutcome::Duplicate(_) => duplicate += 1,
            AppendOutcome::CanonicalCollision(_) => {
                return Err("concurrent identical append collided".to_owned());
            }
        }
    }
    if appended != 1 || duplicate != workers - 1 {
        return Err(format!(
            "concurrent outcomes were appended={appended}, duplicate={duplicate}"
        ));
    }
    Ok(())
}

fn require_outcome(outcome: AppendOutcome, expected: &str) -> Result<(), String> {
    let matches = matches!(
        (&outcome, expected),
        (AppendOutcome::Appended(_), "appended")
            | (AppendOutcome::Duplicate(_), "duplicate")
            | (AppendOutcome::CanonicalCollision(_), "canonical_collision")
    );
    if matches {
        Ok(())
    } else {
        Err(format!("expected {expected}, got {outcome:?}"))
    }
}
