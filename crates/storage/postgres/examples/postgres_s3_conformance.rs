use std::time::Duration;

use chrono::Utc;
use lethe_core::domain::supplemental::InputAnchorSet;
use lethe_core::domain::{
    ActorRef, BlobRef, DataSpaceId, Mutability, ProjectionRef, SupplementalId, SupplementalRecord,
};
use lethe_storage_api::{
    BlobStore, ObservationStore, ProjectionItem, ProjectionItemCommit, ProjectionMaterializer,
    RuntimeStateStore, SupplementalProjectionCommitter, SupplementalStore, conformance,
};
use lethe_storage_postgres::{
    PostgresPersistence, S3BlobStore, S3BlobStoreConfig, S3TransportPolicy,
};
use postgres::{Client, NoTls};

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
        dsn,
        schema,
        expected_role,
        data_space_id,
        read_pool_size,
        endpoint,
        region,
        bucket,
        access_key,
        secret_key,
        corrupt_ref,
    ] = arguments.as_slice()
    else {
        return Err(
            "usage: postgres_s3_conformance <dsn> <schema> <expected-role> <data-space-id> <read-pool-size> <endpoint> <region> <bucket> <access-key> <secret-key> <corrupt-ref>"
                .to_owned(),
        );
    };
    let read_pool_size = read_pool_size
        .parse::<usize>()
        .map_err(|error| format!("read-pool-size must be an unsigned integer: {error}"))?;
    let blob_config = S3BlobStoreConfig {
        endpoint: endpoint.to_owned(),
        region: region.to_owned(),
        bucket: bucket.to_owned(),
        access_key: access_key.to_owned(),
        secret_key: secret_key.to_owned(),
        path_style: true,
        transport_policy: S3TransportPolicy::TestHttp,
        timeout: Duration::from_secs(3),
        max_object_bytes: 4096,
        orphan_min_age: Duration::from_secs(1),
    };
    let store = PostgresPersistence::connect_no_tls(
        DataSpaceId::new(data_space_id),
        dsn,
        schema,
        expected_role,
        read_pool_size,
        blob_config,
    )
    .map_err(|error| error.to_string())?;

    conformance::blob_store_round_trip(&store);
    let admitted = store
        .put_blob(b"admitted-attachment", 4096)
        .map_err(|error| error.to_string())?;
    verify_observation_admission(&store, &admitted)?;
    verify_supplemental_admission(&store, &admitted)?;
    verify_projection_admission(&store, &admitted)?;
    store.deep_check().map_err(|error| error.to_string())?;
    inject_corrupt_metadata(dsn, schema, corrupt_ref)?;
    verify_corrupt_admission(&store, corrupt_ref)?;
    println!("postgres_s3_conformance=passed");
    Ok(())
}

fn verify_observation_admission(
    store: &PostgresPersistence,
    admitted: &BlobRef,
) -> Result<(), String> {
    let mut valid = conformance::sample_observation("postgres:s3:valid-observation");
    valid.attachments = vec![admitted.clone()];
    store
        .append_observation(&valid)
        .map_err(|error| error.to_string())?;

    let missing = BlobRef::new(format!("blob:sha256:{}", "1".repeat(64)));
    let mut rejected = conformance::sample_observation("postgres:s3:missing-observation");
    rejected.attachments = vec![missing];
    if store.append_observation(&rejected).is_ok()
        || store
            .observation_by_id(&rejected.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err("observation with missing S3 attachment was committed".to_owned());
    }
    Ok(())
}

fn verify_supplemental_admission(
    store: &PostgresPersistence,
    admitted: &BlobRef,
) -> Result<(), String> {
    let valid = supplemental("sup:postgres:s3:valid", admitted.clone());
    store
        .put_supplemental(&valid)
        .map_err(|error| error.to_string())?;
    let missing = BlobRef::new(format!("blob:sha256:{}", "2".repeat(64)));
    let rejected = supplemental("sup:postgres:s3:missing", missing.clone());
    if store.put_supplemental(&rejected).is_ok()
        || store
            .supplemental_by_id(&rejected.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err("supplemental with missing S3 reference was committed".to_owned());
    }

    let projection = ProjectionRef::new("proj:postgres:s3:atomic");
    let atomic = supplemental("sup:postgres:s3:atomic", admitted.clone());
    if store
        .commit_supplemental_and_projection(
            &atomic,
            &projection,
            &serde_json::json!({"version": 1}),
            &ProjectionItemCommit::Delta {
                inserts: vec![ProjectionItem {
                    item_key: "missing".to_owned(),
                    owner_key: "owner".to_owned(),
                    sort_key: "001".to_owned(),
                    value: serde_json::json!({"blob_ref": missing.as_str()}),
                }],
                updates: vec![],
                deletes: vec![],
            },
        )
        .is_ok()
        || store
            .supplemental_by_id(&atomic.id)
            .map_err(|error| error.to_string())?
            .is_some()
        || store
            .projection_records(&projection)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err("failed supplemental/projection blob admission leaked state".to_owned());
    }
    Ok(())
}

fn verify_projection_admission(
    store: &PostgresPersistence,
    admitted: &BlobRef,
) -> Result<(), String> {
    let projection = ProjectionRef::new("proj:postgres:s3:valid");
    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"version": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![ProjectionItem {
                    item_key: "valid".to_owned(),
                    owner_key: "owner".to_owned(),
                    sort_key: "001".to_owned(),
                    value: serde_json::json!({"blob_ref": admitted.as_str()}),
                }],
            },
        )
        .map_err(|error| error.to_string())?;
    if !store
        .projection_blob_ref_visible(&projection, admitted)
        .map_err(|error| error.to_string())?
    {
        return Err("valid projection blob reference was not visible".to_owned());
    }
    Ok(())
}

fn verify_corrupt_admission(store: &PostgresPersistence, corrupt_ref: &str) -> Result<(), String> {
    let corrupt = BlobRef::new(corrupt_ref);
    let mut observation = conformance::sample_observation("postgres:s3:corrupt-observation");
    observation.attachments = vec![corrupt];
    if store.append_observation(&observation).is_ok()
        || store
            .observation_by_id(&observation.id)
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err("observation with corrupt S3 attachment was committed".to_owned());
    }
    Ok(())
}

fn inject_corrupt_metadata(dsn: &str, schema: &str, corrupt_ref: &str) -> Result<(), String> {
    let blob_ref = BlobRef::new(corrupt_ref);
    let object_key = S3BlobStore::object_key(&blob_ref).map_err(|error| error.to_string())?;
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
    client
        .execute(
            "INSERT INTO blob_objects (blob_ref, object_key, byte_count)
             VALUES ($1, $2, 17)",
            &[&corrupt_ref, &object_key],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn supplemental(id: &str, blob_ref: BlobRef) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new(id),
        kind: "postgres-s3-conformance".to_owned(),
        derived_from: InputAnchorSet {
            observations: vec![],
            blobs: vec![blob_ref],
            supplementals: vec![],
        },
        payload: serde_json::json!({"result": "fixture"}),
        created_by: ActorRef::new("actor:postgres-s3-conformance"),
        created_at: Utc::now(),
        mutability: Mutability::AppendOnly,
        record_version: Some("1".to_owned()),
        model_version: None,
        consent_metadata: None,
        lineage: None,
    }
}
