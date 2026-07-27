use lethe_core::domain::{DataSpaceId, ProjectionRef};
use lethe_storage_api::{
    AppendOutcome, AuditEventRecord, DiscoveredSlackThread, ObservationStore,
    ProjectionWatermarkStore, SlackThreadCatalogStore, SlackThreadKey, conformance,
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
        return Err(
            "usage: slack_watermark_conformance <dsn> <schema> <expected-role> <data-space-id> <read-pool-size>"
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

    let (thread, append_seq, leaf_id) = verify_slack_catalog(&store)?;
    verify_discovery_atomicity(&store, &thread)?;
    verify_slack_audit_atomicity(&store)?;
    verify_watermark(&store, append_seq, &leaf_id)?;
    println!("slack_watermark_conformance=passed");
    Ok(())
}

fn verify_slack_catalog(
    store: &PostgresPersistence,
) -> Result<(SlackThreadKey, u64, String), String> {
    let thread = SlackThreadKey {
        source_instance: "slack-primary".to_owned(),
        channel_id: "C01ABC".to_owned(),
        thread_ts: "1700000000.000001".to_owned(),
    };
    let observation = conformance::sample_observation("postgres:slack:thread");
    if !matches!(
        store
            .append_slack_observation(&observation, &thread)
            .map_err(|error| error.to_string())?,
        AppendOutcome::Appended(_)
    ) || !matches!(
        store
            .append_slack_observation(&observation, &thread)
            .map_err(|error| error.to_string())?,
        AppendOutcome::Duplicate(_)
    ) {
        return Err("Slack append did not preserve observation idempotency".to_owned());
    }
    let stored = store
        .observation_by_id(&observation.id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "Slack observation was not readable".to_owned())?;
    let catalog = store
        .slack_thread_catalog(&thread.source_instance, &thread.channel_id)
        .map_err(|error| error.to_string())?;
    if catalog.len() != 1
        || catalog[0].key != thread
        || catalog[0].discovered_append_seq != stored.append_seq
    {
        return Err("Slack append did not atomically upsert its thread catalog".to_owned());
    }
    let generation = store
        .advance_slack_thread_poll_generation()
        .map_err(|error| error.to_string())?;
    let due = store
        .slack_threads_to_poll(&thread.source_instance, &thread.channel_id, generation, 10)
        .map_err(|error| error.to_string())?;
    if due.len() != 1 || !due[0].active {
        return Err("active Slack thread was not due immediately".to_owned());
    }
    store
        .complete_slack_thread_poll(
            &thread,
            generation,
            "1700000000.000002",
            false,
            generation + 2,
        )
        .map_err(|error| error.to_string())?;
    let second_generation = store
        .advance_slack_thread_poll_generation()
        .map_err(|error| error.to_string())?;
    if !store
        .slack_threads_to_poll(
            &thread.source_instance,
            &thread.channel_id,
            second_generation,
            10,
        )
        .map_err(|error| error.to_string())?
        .is_empty()
    {
        return Err("inactive Slack thread became due too early".to_owned());
    }
    let due_generation = store
        .advance_slack_thread_poll_generation()
        .map_err(|error| error.to_string())?;
    if store
        .slack_threads_to_poll(
            &thread.source_instance,
            &thread.channel_id,
            due_generation,
            10,
        )
        .map_err(|error| error.to_string())?
        .len()
        != 1
    {
        return Err("inactive Slack thread did not become due on schedule".to_owned());
    }
    Ok((thread, stored.append_seq, stored.leaf_id))
}

fn verify_discovery_atomicity(
    store: &PostgresPersistence,
    thread: &SlackThreadKey,
) -> Result<(), String> {
    let first_tail = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .max_append_seq;
    store
        .commit_slack_thread_discovery(
            first_tail,
            &[DiscoveredSlackThread {
                key: thread.clone(),
                observation_append_seq: first_tail,
            }],
        )
        .map_err(|error| error.to_string())?;
    if store
        .slack_thread_discovery_high_water()
        .map_err(|error| error.to_string())?
        != first_tail
    {
        return Err("Slack discovery high-water did not advance".to_owned());
    }

    let observation = conformance::sample_observation("postgres:slack:discovery");
    store
        .append_observation(&observation)
        .map_err(|error| error.to_string())?;
    let tail = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .max_append_seq;
    let valid = SlackThreadKey {
        source_instance: thread.source_instance.clone(),
        channel_id: thread.channel_id.clone(),
        thread_ts: "1700000000.000003".to_owned(),
    };
    let invalid = SlackThreadKey {
        source_instance: thread.source_instance.clone(),
        channel_id: thread.channel_id.clone(),
        thread_ts: "1700000000.000004".to_owned(),
    };
    if store
        .commit_slack_thread_discovery(
            tail,
            &[
                DiscoveredSlackThread {
                    key: valid.clone(),
                    observation_append_seq: tail,
                },
                DiscoveredSlackThread {
                    key: invalid,
                    observation_append_seq: tail + 1,
                },
            ],
        )
        .is_ok()
    {
        return Err("invalid Slack discovery batch was accepted".to_owned());
    }
    if store
        .slack_thread_discovery_high_water()
        .map_err(|error| error.to_string())?
        != first_tail
        || store
            .slack_thread_catalog(&thread.source_instance, &thread.channel_id)
            .map_err(|error| error.to_string())?
            .iter()
            .any(|entry| entry.key == valid)
    {
        return Err("invalid Slack discovery batch leaked partial state".to_owned());
    }
    Ok(())
}

fn verify_slack_audit_atomicity(store: &PostgresPersistence) -> Result<(), String> {
    let audit = AuditEventRecord {
        id: "audit:slack:duplicate".to_owned(),
        timestamp: "2026-07-27T00:00:00Z".to_owned(),
        actor: "actor:slack-conformance".to_owned(),
        event_json: "{\"event\":\"slack\"}".to_owned(),
    };
    let first = conformance::sample_observation("postgres:slack:audit:first");
    let first_thread = SlackThreadKey {
        source_instance: "slack-primary".to_owned(),
        channel_id: "C01ABC".to_owned(),
        thread_ts: "1700000000.000005".to_owned(),
    };
    store
        .append_slack_observation_with_audit(&first, &first_thread, std::slice::from_ref(&audit))
        .map_err(|error| error.to_string())?;
    let rolled_back = conformance::sample_observation("postgres:slack:audit:rollback");
    let rolled_back_thread = SlackThreadKey {
        source_instance: "slack-primary".to_owned(),
        channel_id: "C01ABC".to_owned(),
        thread_ts: "1700000000.000006".to_owned(),
    };
    if store
        .append_slack_observation_with_audit(
            &rolled_back,
            &rolled_back_thread,
            std::slice::from_ref(&audit),
        )
        .is_ok()
    {
        return Err("duplicate Slack audit id did not fail".to_owned());
    }
    if store
        .observation_by_id(&rolled_back.id)
        .map_err(|error| error.to_string())?
        .is_some()
        || store
            .slack_thread_catalog(
                &rolled_back_thread.source_instance,
                &rolled_back_thread.channel_id,
            )
            .map_err(|error| error.to_string())?
            .iter()
            .any(|entry| entry.key == rolled_back_thread)
    {
        return Err(
            "failed Slack audit transaction leaked observation or catalog state".to_owned(),
        );
    }
    Ok(())
}

fn verify_watermark(
    store: &PostgresPersistence,
    append_seq: u64,
    leaf_id: &str,
) -> Result<(), String> {
    let projection = ProjectionRef::new("proj:postgres:watermark");
    let mut watermark = store
        .projection_leaf_watermark(&projection, leaf_id)
        .map_err(|error| error.to_string())?;
    if watermark.append_seq != 0 || watermark.status != "success" {
        return Err("new projection leaf watermark has the wrong initial state".to_owned());
    }
    watermark.append_seq = append_seq;
    store
        .commit_projection_leaf_watermark(&watermark)
        .map_err(|error| error.to_string())?;
    if store
        .projection_leaf_watermark(&projection, leaf_id)
        .map_err(|error| error.to_string())?
        != watermark
    {
        return Err("projection leaf watermark did not round-trip".to_owned());
    }
    watermark.append_seq -= 1;
    if store.commit_projection_leaf_watermark(&watermark).is_ok() {
        return Err("projection leaf watermark regression was accepted".to_owned());
    }
    watermark.append_seq = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .max_append_seq
        + 1;
    if store.commit_projection_leaf_watermark(&watermark).is_ok() {
        return Err("projection leaf watermark beyond its leaf tail was accepted".to_owned());
    }
    Ok(())
}
