use chrono::Utc;
use lethe_core::domain::supplemental::InputAnchorSet;
use lethe_core::domain::{
    ActorRef, DataSpaceId, Mutability, ProjectionRef, SupplementalId, SupplementalRecord,
};
use lethe_storage_api::{
    AuditEventRecord, ProjectionItem, ProjectionItemCommit, ProjectionMaterializer,
    SupplementalProjectionCommitter, SupplementalStore, conformance,
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
            "usage: projection_conformance <dsn> <schema> <expected-role> <data-space-id> <read-pool-size>"
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

    conformance::materializer_round_trip(&store);
    verify_supplementals(&store)?;
    verify_atomic_commit(&store)?;
    verify_failed_publish_is_atomic(&store)?;
    verify_generation_cleanup(&store)?;
    println!("projection_conformance=passed");
    Ok(())
}

fn verify_supplementals(store: &PostgresPersistence) -> Result<(), String> {
    let first = supplemental("sup:postgres:page:first", 1);
    let second = supplemental("sup:postgres:page:second", 2);
    store
        .put_supplemental(&first)
        .map_err(|error| error.to_string())?;
    store
        .put_supplemental(&second)
        .map_err(|error| error.to_string())?;
    if store
        .supplemental_by_id(&first.id)
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return Err("supplemental was not readable by id".to_owned());
    }
    let page = store
        .supplemental_page(Some(&first.created_at.to_rfc3339()), 10)
        .map_err(|error| error.to_string())?;
    if page.len() != 1 || page[0].id != second.id {
        return Err("supplemental keyset page did not return the second record".to_owned());
    }
    Ok(())
}

fn verify_atomic_commit(store: &PostgresPersistence) -> Result<(), String> {
    let projection = ProjectionRef::new("proj:postgres:atomic");
    let record = supplemental("sup:postgres:atomic", 3);
    let insert = item("atomic-item", "owner-a", "001", "committed");
    store
        .commit_supplemental_and_projection(
            &record,
            &projection,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Delta {
                inserts: vec![insert.clone()],
                updates: vec![],
                deletes: vec![],
            },
        )
        .map_err(|error| error.to_string())?;
    if store
        .supplemental_by_id(&record.id)
        .map_err(|error| error.to_string())?
        .is_none()
        || store
            .projection_item_by_key(&projection, &insert.item_key)
            .map_err(|error| error.to_string())?
            != Some(insert)
    {
        return Err("atomic supplemental/projection commit was not visible".to_owned());
    }

    let rolled_back = supplemental("sup:postgres:atomic:rollback", 4);
    let audit = AuditEventRecord {
        id: "audit:postgres:atomic".to_owned(),
        timestamp: "2026-07-27T00:00:00Z".to_owned(),
        actor: "actor:postgres-conformance".to_owned(),
        event_json: "{\"event\":\"projection-commit\"}".to_owned(),
    };
    store
        .commit_supplemental_and_projection_with_audit(
            &supplemental("sup:postgres:atomic:audit-seed", 5),
            &projection,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Delta {
                inserts: vec![item("audit-seed", "owner-a", "002", "seed")],
                updates: vec![],
                deletes: vec![],
            },
            &audit,
        )
        .map_err(|error| error.to_string())?;
    let before = store
        .projection_records(&projection)
        .map_err(|error| error.to_string())?;
    let failed_item = item("must-not-commit", "owner-a", "003", "rollback");
    if store
        .commit_supplemental_and_projection_with_audit(
            &rolled_back,
            &projection,
            &serde_json::json!({"generation": 3}),
            &ProjectionItemCommit::Delta {
                inserts: vec![failed_item.clone()],
                updates: vec![],
                deletes: vec![],
            },
            &audit,
        )
        .is_ok()
    {
        return Err("duplicate audit id did not fail the atomic commit".to_owned());
    }
    if store
        .supplemental_by_id(&rolled_back.id)
        .map_err(|error| error.to_string())?
        .is_some()
        || store
            .projection_item_by_key(&projection, &failed_item.item_key)
            .map_err(|error| error.to_string())?
            .is_some()
        || store
            .projection_records(&projection)
            .map_err(|error| error.to_string())?
            != before
    {
        return Err("failed atomic commit leaked supplemental or projection state".to_owned());
    }
    Ok(())
}

fn verify_failed_publish_is_atomic(store: &PostgresPersistence) -> Result<(), String> {
    let target = ProjectionRef::new("proj:postgres:publish-target");
    let staging = ProjectionRef::new("proj:postgres:publish-staging");
    let old = item("old", "owner-a", "001", "old");
    let candidate = item("candidate", "owner-a", "001", "candidate");
    store
        .commit_projection_items(
            &target,
            &serde_json::json!({"version": "old"}),
            &ProjectionItemCommit::Replace {
                items: vec![old.clone()],
            },
        )
        .map_err(|error| error.to_string())?;
    store
        .commit_projection_items(
            &staging,
            &serde_json::json!({"version": "staging"}),
            &ProjectionItemCommit::Replace {
                items: vec![candidate.clone()],
            },
        )
        .map_err(|error| error.to_string())?;
    if store
        .publish_projection_items_from_staging(
            &target,
            &staging,
            &serde_json::json!({"version": "new"}),
            2,
        )
        .is_ok()
    {
        return Err("publish accepted an incorrect staging item count".to_owned());
    }
    if store
        .projection_item_by_key(&target, &old.item_key)
        .map_err(|error| error.to_string())?
        != Some(old)
        || store
            .projection_item_by_key(&target, &candidate.item_key)
            .map_err(|error| error.to_string())?
            .is_some()
        || store
            .projection_item_by_key(&staging, &candidate.item_key)
            .map_err(|error| error.to_string())?
            != Some(candidate)
    {
        return Err("failed staging publish changed a visible generation".to_owned());
    }
    Ok(())
}

fn verify_generation_cleanup(store: &PostgresPersistence) -> Result<(), String> {
    let mut saw_retired = false;
    for _ in 0..100 {
        let result = store
            .cleanup_retired_projection_generation(1)
            .map_err(|error| error.to_string())?;
        saw_retired |= result.storage_projection_id.is_some();
        if !result.has_more {
            if !saw_retired {
                return Err("projection conformance did not create retired generations".to_owned());
            }
            return Ok(());
        }
    }
    Err("retired projection cleanup did not converge".to_owned())
}

fn supplemental(id: &str, second: u32) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new(id),
        kind: "projection-conformance".to_owned(),
        derived_from: InputAnchorSet::default(),
        payload: serde_json::json!({"second": second}),
        created_by: ActorRef::new("actor:postgres-conformance"),
        created_at: chrono::DateTime::parse_from_rfc3339(&format!("2026-07-27T00:00:{second:02}Z"))
            .expect("fixed conformance timestamp must parse")
            .with_timezone(&Utc),
        mutability: Mutability::AppendOnly,
        record_version: Some("1".to_owned()),
        model_version: None,
        consent_metadata: None,
        lineage: None,
    }
}

fn item(item_key: &str, owner_key: &str, sort_key: &str, body: &str) -> ProjectionItem {
    ProjectionItem {
        item_key: item_key.to_owned(),
        owner_key: owner_key.to_owned(),
        sort_key: sort_key.to_owned(),
        value: serde_json::json!({"body": body}),
    }
}
