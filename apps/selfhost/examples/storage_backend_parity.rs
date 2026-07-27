use std::path::PathBuf;
use std::time::Duration;

use lethe_core::domain::{
    BlobRef, DataSpaceId, IdempotencyKey, Observation, ProjectionRef, SchemaRef, SemVer,
};
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_selfhost::self_host::app::{
    SourceObservationExportQuery, export_source_observation_page,
};
use lethe_storage_api::{
    AppendOutcome, AuditEventRecord, CutoverFixture, ProjectionItem, ProjectionItemCommit,
    StorageError, StoragePorts, conformance,
};
use lethe_storage_postgres::{PostgresPersistence, S3BlobStoreConfig, S3TransportPolicy};
use lethe_storage_sqlite::persistence::SqlitePersistence;
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
    let [
        _,
        sqlite_root,
        dsn,
        schema,
        role,
        data_space_id,
        endpoint,
        bucket,
        access_key,
        secret_key,
    ] = arguments.as_slice()
    else {
        return Err(
            "usage: storage_backend_parity <sqlite-root> <dsn> <schema> <role> <data-space-id> <endpoint> <bucket> <access-key> <secret-key>"
                .to_owned(),
        );
    };
    let sqlite_root = PathBuf::from(sqlite_root);
    std::fs::create_dir_all(&sqlite_root)
        .map_err(|error| format!("creating SQLite parity root: {error}"))?;
    let sqlite_database = sqlite_root.join("general.sqlite3");
    let sqlite_blobs = sqlite_root.join("blobs");
    let sqlite = SqlitePersistence::open_with_routing_key_order(
        &sqlite_database,
        &sqlite_blobs,
        &[31; 32],
        RoutingKeyOrder::YearMonthSourceContainerPublished,
    )
    .map_err(|error| error.to_string())?;
    let postgres = open_postgres(
        dsn,
        schema,
        role,
        data_space_id,
        endpoint,
        bucket,
        access_key,
        secret_key,
    )?;

    let fixture = ParityFixture::new();
    let sqlite_outcomes = write_fixture(&sqlite, &fixture)?;
    let postgres_outcomes = write_fixture(&postgres, &fixture)?;
    if sqlite_outcomes != postgres_outcomes {
        return Err(format!(
            "normalized append outcomes differ: SQLite={sqlite_outcomes:?}, PostgreSQL={postgres_outcomes:?}"
        ));
    }
    let sqlite_before_atomic = read_snapshot(&sqlite, &fixture.blob_ref)?;
    let postgres_before_atomic = read_snapshot(&postgres, &fixture.blob_ref)?;
    if sqlite_before_atomic != postgres_before_atomic {
        return Err("normalized backend snapshots differ before atomic fixtures".to_owned());
    }

    let atomic_fixture = AtomicParityFixture::new();
    let sqlite_atomic =
        write_atomic_fixture(&sqlite, &atomic_fixture, |mismatch, identity, json| {
            inject_sqlite_collision_candidate(&sqlite_database, mismatch, identity, json)
        })?;
    let postgres_atomic =
        write_atomic_fixture(&postgres, &atomic_fixture, |mismatch, identity, json| {
            inject_postgres_collision_candidate(dsn, schema, mismatch, identity, json)
        })?;
    if sqlite_atomic != postgres_atomic {
        return Err(format!(
            "normalized atomic-page results differ: SQLite={sqlite_atomic:?}, PostgreSQL={postgres_atomic:?}"
        ));
    }
    let sqlite_before_restart =
        normalize_post_atomic_snapshot(read_snapshot(&sqlite, &fixture.blob_ref)?);
    let postgres_before_restart =
        normalize_post_atomic_snapshot(read_snapshot(&postgres, &fixture.blob_ref)?);
    if sqlite_before_restart != postgres_before_restart {
        return Err("normalized backend snapshots differ before restart".to_owned());
    }
    let sqlite_export_before_restart = read_source_export(&sqlite)?;
    let postgres_export_before_restart = read_source_export(&postgres)?;
    if sqlite_export_before_restart != postgres_export_before_restart {
        return Err(format!(
            "normalized source Observation export differs before restart: SQLite={sqlite_export_before_restart:?}, PostgreSQL={postgres_export_before_restart:?}"
        ));
    }
    drop(sqlite);
    drop(postgres);

    let sqlite = SqlitePersistence::open_with_routing_key_order(
        &sqlite_database,
        &sqlite_blobs,
        &[31; 32],
        RoutingKeyOrder::YearMonthSourceContainerPublished,
    )
    .map_err(|error| error.to_string())?;
    if sqlite.schema_migrations_applied_on_open() {
        return Err("SQLite restart unexpectedly reapplied schema migrations".to_owned());
    }
    let postgres = open_postgres(
        dsn,
        schema,
        role,
        data_space_id,
        endpoint,
        bucket,
        access_key,
        secret_key,
    )?;
    if !postgres.migration_outcome().applied_versions.is_empty() {
        return Err("PostgreSQL restart unexpectedly reapplied schema migrations".to_owned());
    }
    let sqlite_after_restart =
        normalize_post_atomic_snapshot(read_snapshot(&sqlite, &fixture.blob_ref)?);
    let postgres_after_restart =
        normalize_post_atomic_snapshot(read_snapshot(&postgres, &fixture.blob_ref)?);
    let sqlite_export_after_restart = read_source_export(&sqlite)?;
    let postgres_export_after_restart = read_source_export(&postgres)?;
    if sqlite_after_restart != sqlite_before_restart
        || postgres_after_restart != postgres_before_restart
        || sqlite_after_restart != postgres_after_restart
        || sqlite_export_after_restart != sqlite_export_before_restart
        || postgres_export_after_restart != postgres_export_before_restart
        || sqlite_export_after_restart != postgres_export_after_restart
    {
        return Err("restart/replay changed a normalized backend snapshot".to_owned());
    }
    println!("storage_backend_parity=passed");
    Ok(())
}

struct ParityFixture {
    blob_bytes: Vec<u8>,
    blob_ref: BlobRef,
    first: Observation,
    second: Observation,
    collision: Observation,
}

#[derive(Debug, PartialEq, Eq)]
struct AtomicParityReport {
    success: Vec<&'static str>,
    retry: Vec<&'static str>,
    stale_generation: &'static str,
    missing_blob: &'static str,
    audit_failure: &'static str,
    collision: &'static str,
    success_count_delta: u64,
    rejected_pages_preserved_count: bool,
    page_audit_delta: usize,
    v2_ingested: u64,
}

struct AtomicParityFixture {
    unit: &'static str,
    canary: Observation,
    canary_object_id: &'static str,
    canary_canonical_json: String,
    canary_identity: String,
    success: Vec<Observation>,
    stale: Observation,
    missing_blob: Observation,
    audit_failure: Observation,
    collision_prefix: Observation,
    collision: Observation,
    mismatch: Observation,
    mismatch_canonical_json: String,
}

#[derive(Debug, PartialEq, Eq)]
struct NormalizedSourceExportPage {
    watermark: u64,
    next_after_append_seq: u64,
    complete: bool,
    items: Vec<(u64, String)>,
}

fn read_source_export(store: &dyn StoragePorts) -> Result<Vec<NormalizedSourceExportPage>, String> {
    let mut pages = Vec::new();
    let mut after_append_seq = 0;
    let mut watermark = None;
    loop {
        let page = export_source_observation_page(
            store,
            &SourceObservationExportQuery {
                after_append_seq,
                limit: 2,
                watermark,
            },
            2,
            10_000,
        )
        .map_err(|error| error.to_string())?;
        let normalized = NormalizedSourceExportPage {
            watermark: page.watermark,
            next_after_append_seq: page.next_after_append_seq,
            complete: page.complete,
            items: page
                .items
                .into_iter()
                .map(|item| {
                    serde_json::to_string(&item.observation)
                        .map(|observation| (item.append_seq, observation))
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        if normalized.next_after_append_seq < after_append_seq {
            return Err("source Observation export cursor moved backwards".to_owned());
        }
        after_append_seq = normalized.next_after_append_seq;
        watermark = Some(normalized.watermark);
        let complete = normalized.complete;
        pages.push(normalized);
        if complete {
            return Ok(pages);
        }
    }
}

impl AtomicParityFixture {
    fn new() -> Self {
        let unit = "atomic-parity";
        let canary_object_id = "canary";
        let canary_canonical_json = serde_json::json!({"body": "canary"}).to_string();
        let canary_identity = atomic_identity(unit, canary_object_id, &canary_canonical_json);
        let canary = atomic_observation(
            unit,
            canary_object_id,
            &canary_canonical_json,
            "atomic-parity:legacy:canary",
        );
        let success = ["success-a", "success-b"]
            .into_iter()
            .map(|object_id| {
                let canonical_json = serde_json::json!({"body": object_id}).to_string();
                atomic_observation(
                    unit,
                    object_id,
                    &canonical_json,
                    &atomic_identity(unit, object_id, &canonical_json),
                )
            })
            .collect();
        let stale_json = serde_json::json!({"body": "stale"}).to_string();
        let stale = atomic_observation(
            unit,
            "stale",
            &stale_json,
            &atomic_identity(unit, "stale", &stale_json),
        );
        let missing_json = serde_json::json!({"body": "missing blob"}).to_string();
        let mut missing_blob = atomic_observation(
            unit,
            "missing-blob",
            &missing_json,
            &atomic_identity(unit, "missing-blob", &missing_json),
        );
        missing_blob.attachments = vec![BlobRef::new(format!("blob:sha256:{}", "0".repeat(64)))];
        let audit_json = serde_json::json!({"body": "audit failure"}).to_string();
        let audit_failure = atomic_observation(
            unit,
            "audit-failure",
            &audit_json,
            &atomic_identity(unit, "audit-failure", &audit_json),
        );
        let collision_prefix_json = serde_json::json!({"body": "collision prefix"}).to_string();
        let collision_prefix = atomic_observation(
            unit,
            "collision-prefix",
            &collision_prefix_json,
            &atomic_identity(unit, "collision-prefix", &collision_prefix_json),
        );
        let mut collision = canary.clone();
        collision.id = Observation::new_id();
        collision.idempotency_key = IdempotencyKey::new(canary_identity.clone());
        let mismatch_canonical_json = serde_json::json!({"body": "forced mismatch"}).to_string();
        let mismatch = atomic_observation(
            unit,
            "mismatch",
            &mismatch_canonical_json,
            "atomic-parity:legacy:mismatch",
        );
        Self {
            unit,
            canary,
            canary_object_id,
            canary_canonical_json,
            canary_identity,
            success,
            stale,
            missing_blob,
            audit_failure,
            collision_prefix,
            collision,
            mismatch,
            mismatch_canonical_json,
        }
    }
}

impl ParityFixture {
    fn new() -> Self {
        let blob_bytes = b"storage-backend-parity-blob".to_vec();
        let blob_ref = BlobRef::new(format!(
            "blob:sha256:{}",
            hex::encode(Sha256::digest(&blob_bytes))
        ));
        let mut first = conformance::sample_observation("parity:first");
        first.attachments = vec![blob_ref.clone()];
        first.meta["source_instance"] = serde_json::json!("parity-base");
        first.meta["object_id"] = serde_json::json!("first");
        let mut second = conformance::sample_observation("parity:second");
        second.meta["source_instance"] = serde_json::json!("parity-base");
        second.meta["object_id"] = serde_json::json!("second");
        let mut collision = first.clone();
        collision.id = Observation::new_id();
        collision.payload = serde_json::json!({"value": "collision"});
        collision.meta["canonical_json"] =
            serde_json::Value::String("{\"value\":\"collision\"}".to_owned());
        Self {
            blob_bytes,
            blob_ref,
            first,
            second,
            collision,
        }
    }
}

fn write_atomic_fixture(
    store: &dyn StoragePorts,
    fixture: &AtomicParityFixture,
    inject_collision: impl FnOnce(&Observation, &str, &str) -> Result<(), String>,
) -> Result<AtomicParityReport, String> {
    store
        .append_observation(&fixture.canary)
        .map_err(|error| error.to_string())?;
    loop {
        let report = store
            .identity_bridge_apply_batch(4096)
            .map_err(|error| error.to_string())?;
        if report.read_count == 0 {
            break;
        }
    }
    store
        .cutover_register(fixture.unit, "owner:parity", "register atomic parity")
        .map_err(|error| error.to_string())?;
    store
        .cutover_begin_drain(fixture.unit, "owner:parity", "fence atomic parity")
        .map_err(|error| error.to_string())?;
    let cutover_fixture = CutoverFixture {
        object_id: fixture.canary_object_id.to_owned(),
        canonical_json: fixture.canary_canonical_json.clone(),
        expected_identity_key: fixture.canary_identity.clone(),
        expected_observation_id: Some(fixture.canary.id.clone()),
    };
    let active = store
        .cutover_activate(
            fixture.unit,
            "owner:parity",
            "activate atomic parity",
            &cutover_fixture,
        )
        .map_err(|error| error.to_string())?;
    if active.generation != 2 {
        return Err("atomic parity cutover did not activate generation 2".to_owned());
    }

    let count_before = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count;
    let audits_before = store
        .audit_event_page(None, 10_000)
        .map_err(|error| error.to_string())?
        .len();
    let success_audit = atomic_audit("audit:atomic-parity-success");
    let success = store
        .append_observations_v2_atomic_page(fixture.unit, 2, &fixture.success, &[success_audit])
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(outcome_name)
        .collect::<Vec<_>>();
    let retry_audit = atomic_audit("audit:atomic-parity-retry");
    let retry = store
        .append_observations_v2_atomic_page(fixture.unit, 2, &fixture.success, &[retry_audit])
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(outcome_name)
        .collect::<Vec<_>>();
    let count_after_success = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count;

    let stale_generation = normalized_error(store.append_observations_v2_atomic_page(
        fixture.unit,
        1,
        std::slice::from_ref(&fixture.stale),
        &[],
    ));
    let missing_blob = normalized_error(store.append_observations_v2_atomic_page(
        fixture.unit,
        2,
        std::slice::from_ref(&fixture.missing_blob),
        &[],
    ));
    let duplicate_audit = atomic_audit("audit:atomic-parity-duplicate");
    let audit_failure = normalized_error(store.append_observations_v2_atomic_page(
        fixture.unit,
        2,
        std::slice::from_ref(&fixture.audit_failure),
        &[duplicate_audit.clone(), duplicate_audit],
    ));
    let count_after_rejections = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count;

    store
        .append_observation(&fixture.mismatch)
        .map_err(|error| error.to_string())?;
    inject_collision(
        &fixture.mismatch,
        &fixture.canary_identity,
        &fixture.mismatch_canonical_json,
    )?;
    let before_collision = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count;
    let collision = normalized_error(store.append_observations_v2_atomic_page(
        fixture.unit,
        2,
        &[fixture.collision_prefix.clone(), fixture.collision.clone()],
        &[atomic_audit("audit:atomic-parity-collision")],
    ));
    let after_collision = store
        .observation_stats()
        .map_err(|error| error.to_string())?
        .count;
    let audits_after = store
        .audit_event_page(None, 10_000)
        .map_err(|error| error.to_string())?
        .len();
    let health = store
        .cutover_health(fixture.unit)
        .map_err(|error| error.to_string())?;

    Ok(AtomicParityReport {
        success,
        retry,
        stale_generation,
        missing_blob,
        audit_failure,
        collision,
        success_count_delta: count_after_success.saturating_sub(count_before),
        rejected_pages_preserved_count: count_after_rejections == count_after_success
            && after_collision == before_collision,
        page_audit_delta: audits_after.saturating_sub(audits_before),
        v2_ingested: health.state.v2_ingested,
    })
}

fn atomic_observation(
    source_instance_id: &str,
    object_id: &str,
    canonical_json: &str,
    identity_key: &str,
) -> Observation {
    let mut observation = conformance::sample_observation(identity_key);
    observation.schema = SchemaRef::new("schema:askbot-source-observation");
    observation.schema_version = SemVer::new("1.0.0");
    observation.idempotency_key = IdempotencyKey::new(identity_key);
    observation.meta = serde_json::json!({
        "canonical_json": canonical_json,
        "source_instance": source_instance_id,
        "object_id": object_id,
        "source_container": "atomic-parity",
    });
    observation
}

fn atomic_identity(source_instance_id: &str, object_id: &str, canonical_json: &str) -> String {
    format!(
        "{source_instance_id}:{object_id}:{}",
        hex::encode(Sha256::digest(canonical_json.as_bytes()))
    )
}

fn atomic_audit(id: &str) -> AuditEventRecord {
    AuditEventRecord {
        id: id.to_owned(),
        timestamp: "2026-07-27T00:00:00Z".to_owned(),
        actor: "actor:parity".to_owned(),
        event_json: serde_json::json!({"mode": "atomic_backend_parity"}).to_string(),
    }
}

fn normalized_error<T>(result: Result<T, StorageError>) -> &'static str {
    match result {
        Ok(_) => "unexpected_success",
        Err(StorageError::Backend(_)) => "backend",
        Err(StorageError::Invariant(_)) => "invariant",
        Err(StorageError::CutoverAdmissionDenied(_)) => "cutover_admission_denied",
        Err(StorageError::AtomicPageCollision { index: 1, .. }) => "atomic_collision_index_1",
        Err(StorageError::AtomicPageCollision { .. }) => "atomic_collision_wrong_index",
        Err(StorageError::CutoverConflict(_)) => "cutover_conflict",
        Err(StorageError::CutoverRollbackRefused(_)) => "cutover_rollback_refused",
        Err(StorageError::OperationalIdempotencyCollision(_)) => {
            "operational_idempotency_collision"
        }
        Err(StorageError::OperationalEventIdCollision(_)) => "operational_event_id_collision",
    }
}

fn inject_sqlite_collision_candidate(
    database_path: &std::path::Path,
    mismatch: &Observation,
    identity: &str,
    canonical_json: &str,
) -> Result<(), String> {
    let connection =
        rusqlite::Connection::open(database_path).map_err(|error| error.to_string())?;
    let append_seq = connection
        .query_row(
            "SELECT append_seq FROM observations WHERE id = ?1",
            [mismatch.id.as_str()],
            |row| row.get::<_, u64>(0),
        )
        .map_err(|error| error.to_string())?;
    connection
        .execute(
            "INSERT INTO identity_bridge_candidates (
                v2_identity_key, observation_id, source_instance_id, append_seq,
                canonical_json, canonical_json_sha256
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                identity,
                mismatch.id.as_str(),
                "atomic-parity",
                append_seq,
                canonical_json,
                hex::encode(Sha256::digest(canonical_json.as_bytes())),
            ],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn inject_postgres_collision_candidate(
    dsn: &str,
    schema: &str,
    mismatch: &Observation,
    identity: &str,
    canonical_json: &str,
) -> Result<(), String> {
    if !schema
        .chars()
        .all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err("parity schema contains an unsafe identifier".to_owned());
    }
    let mut client = Client::connect(dsn, NoTls).map_err(|error| error.to_string())?;
    client
        .batch_execute(&format!("SET search_path TO \"{schema}\", pg_catalog"))
        .map_err(|error| error.to_string())?;
    let append_seq: i64 = client
        .query_one(
            "SELECT append_seq FROM observations WHERE observation_id = $1",
            &[&mismatch.id.as_str()],
        )
        .map_err(|error| error.to_string())?
        .get(0);
    let digest = hex::encode(Sha256::digest(canonical_json.as_bytes()));
    client
        .execute(
            "INSERT INTO identity_bridge_candidates (
                v2_identity_key, observation_id, source_instance_id, append_seq,
                canonical_json, canonical_json_sha256
             ) VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &identity,
                &mismatch.id.as_str(),
                &"atomic-parity",
                &append_seq,
                &canonical_json,
                &digest,
            ],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_fixture(
    store: &dyn StoragePorts,
    fixture: &ParityFixture,
) -> Result<Vec<&'static str>, String> {
    let blob_ref = store
        .put_blob(&fixture.blob_bytes, 4096)
        .map_err(|error| error.to_string())?;
    if blob_ref != fixture.blob_ref {
        return Err("backend minted a non-canonical blob reference".to_owned());
    }
    let outcomes = [
        store
            .append_observation(&fixture.first)
            .map_err(|error| error.to_string())?,
        store
            .append_observation(&fixture.first)
            .map_err(|error| error.to_string())?,
        store
            .append_observation(&fixture.collision)
            .map_err(|error| error.to_string())?,
        store
            .append_observation(&fixture.second)
            .map_err(|error| error.to_string())?,
    ]
    .into_iter()
    .map(outcome_name)
    .collect::<Vec<_>>();
    store
        .commit_projection_items(
            &ProjectionRef::new("proj:parity"),
            &serde_json::json!({"format_version": 1, "kind": "parity"}),
            &ProjectionItemCommit::Replace {
                items: vec![ProjectionItem {
                    item_key: "parity:item".to_owned(),
                    owner_key: "parity:owner".to_owned(),
                    sort_key: "001".to_owned(),
                    value: serde_json::json!({"blob_ref": fixture.blob_ref.as_str()}),
                }],
            },
        )
        .map_err(|error| error.to_string())?;
    store
        .set_state("parity-state", "ready")
        .map_err(|error| error.to_string())?;
    Ok(outcomes)
}

fn read_snapshot(
    store: &dyn StoragePorts,
    blob_ref: &BlobRef,
) -> Result<serde_json::Value, String> {
    let observations = store
        .load_observations()
        .map_err(|error| error.to_string())?;
    let stats = store
        .observation_stats()
        .map_err(|error| error.to_string())?;
    let mut items = store
        .projection_items_by_owner(&ProjectionRef::new("proj:parity"), "parity:owner")
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|item| {
            serde_json::json!({
                "item_key": item.item_key,
                "owner_key": item.owner_key,
                "sort_key": item.sort_key,
                "value": item.value,
            })
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left["item_key"].as_str().cmp(&right["item_key"].as_str()));
    let blob = store
        .get_blob(blob_ref)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "parity blob disappeared".to_owned())?;
    Ok(serde_json::json!({
        "observations": observations,
        "stats": {
            "count": stats.count,
            "max_append_seq": stats.max_append_seq,
        },
        "projection_manifest": store
            .projection_records(&ProjectionRef::new("proj:parity"))
            .map_err(|error| error.to_string())?,
        "projection_items": items,
        "runtime_state": store
            .get_state("parity-state")
            .map_err(|error| error.to_string())?,
        "blob_hex": hex::encode(blob),
    }))
}

fn normalize_post_atomic_snapshot(mut snapshot: serde_json::Value) -> serde_json::Value {
    if let Some(stats) = snapshot
        .get_mut("stats")
        .and_then(serde_json::Value::as_object_mut)
    {
        // PostgreSQL sequences are non-transactional while SQLite rowids are
        // reused after rollback. The visible ledger count and ordered records
        // are the cross-backend contract; an allocator's skipped value is not.
        stats.remove("max_append_seq");
    }
    snapshot
}

fn outcome_name(outcome: AppendOutcome) -> &'static str {
    match outcome {
        AppendOutcome::Appended(_) => "appended",
        AppendOutcome::Duplicate(_) => "duplicate",
        AppendOutcome::CanonicalCollision(_) => "canonical_collision",
    }
}

#[allow(clippy::too_many_arguments)]
fn open_postgres(
    dsn: &str,
    schema: &str,
    role: &str,
    data_space_id: &str,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<PostgresPersistence, String> {
    PostgresPersistence::connect_no_tls(
        DataSpaceId::new(data_space_id),
        dsn,
        schema,
        role,
        2,
        S3BlobStoreConfig {
            endpoint: endpoint.to_owned(),
            region: "us-east-1".to_owned(),
            bucket: bucket.to_owned(),
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
            path_style: true,
            transport_policy: S3TransportPolicy::TestHttp,
            timeout: Duration::from_secs(3),
            max_object_bytes: 4096,
            orphan_min_age: Duration::from_secs(3),
        },
    )
    .map_err(|error| error.to_string())
}
