use std::thread;
use std::time::Duration;

use lethe_core::domain::{BlobRef, DataSpaceId};
use lethe_storage_api::{BlobStore, ObservationStore, RuntimeStateStore, conformance};
use lethe_storage_postgres::{
    PostgresPersistence, S3BlobStore, S3BlobStoreConfig, S3TransportPolicy,
};
use postgres::{Client, NoTls};

const ORPHAN_MIN_AGE: Duration = Duration::from_secs(3);

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
    ] = arguments.as_slice()
    else {
        return Err(
            "usage: orphan_gc_conformance <dsn> <schema> <expected-role> <data-space-id> <read-pool-size> <endpoint> <region> <bucket> <access-key> <secret-key>"
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
        orphan_min_age: ORPHAN_MIN_AGE,
    };
    let direct_s3 = S3BlobStore::connect(blob_config.clone()).map_err(|error| error.to_string())?;
    let store = PostgresPersistence::connect_no_tls(
        DataSpaceId::new(data_space_id),
        dsn,
        schema,
        expected_role,
        read_pool_size,
        blob_config,
    )
    .map_err(|error| error.to_string())?;

    let referenced = store
        .put_blob(b"gc-referenced", 4096)
        .map_err(|error| error.to_string())?;
    append_with_attachment(&store, "postgres:s3:gc-referenced", &referenced)?;
    let young = store
        .put_blob(b"gc-young", 4096)
        .map_err(|error| error.to_string())?;
    let orphan = store
        .put_blob(b"gc-orphan", 4096)
        .map_err(|error| error.to_string())?;

    expect_deleted(&store, 0, "first young scan")?;
    expect_deleted(&store, 0, "second young scan")?;
    require_present(&store, &direct_s3, &young, "young object")?;
    append_with_attachment(&store, "postgres:s3:gc-young-preserved", &young)?;

    thread::sleep(ORPHAN_MIN_AGE + Duration::from_secs(1));
    expect_deleted(&store, 0, "first old-enough scan")?;
    require_present(&store, &direct_s3, &orphan, "once-seen orphan")?;

    install_audit_failure(dsn, schema)?;
    if store.garbage_collect_orphan_blobs().is_ok() {
        return Err("audit-failed orphan collection unexpectedly succeeded".to_owned());
    }
    require_present(&store, &direct_s3, &orphan, "audit-failed orphan")?;
    remove_audit_failure(dsn, schema)?;

    expect_deleted(&store, 1, "eligible orphan scan")?;
    if store
        .get_blob(&orphan)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("deleted orphan retained PostgreSQL metadata".to_owned());
    }
    if direct_s3
        .get_blob(&orphan)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("deleted orphan remained in S3".to_owned());
    }
    require_present(&store, &direct_s3, &referenced, "referenced object")?;
    require_present(&store, &direct_s3, &young, "newly referenced young object")?;
    require_delete_audit(dsn, schema, &orphan)?;
    println!("orphan_gc_conformance=passed");
    Ok(())
}

fn append_with_attachment(
    store: &PostgresPersistence,
    id: &str,
    blob_ref: &BlobRef,
) -> Result<(), String> {
    let mut observation = conformance::sample_observation(id);
    observation.attachments = vec![blob_ref.clone()];
    store
        .append_observation(&observation)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn expect_deleted(store: &PostgresPersistence, expected: usize, stage: &str) -> Result<(), String> {
    let deleted = store
        .garbage_collect_orphan_blobs()
        .map_err(|error| format!("{stage}: {error}"))?;
    if deleted != expected {
        return Err(format!(
            "{stage} deleted {deleted} objects; expected {expected}"
        ));
    }
    Ok(())
}

fn require_present(
    store: &PostgresPersistence,
    direct_s3: &S3BlobStore,
    blob_ref: &BlobRef,
    label: &str,
) -> Result<(), String> {
    if store
        .get_blob(blob_ref)
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return Err(format!("{label} lost PostgreSQL metadata"));
    }
    if direct_s3
        .get_blob(blob_ref)
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return Err(format!("{label} disappeared from S3"));
    }
    Ok(())
}

fn install_audit_failure(dsn: &str, schema: &str) -> Result<(), String> {
    let mut client = schema_client(dsn, schema)?;
    client
        .batch_execute(
            "
            CREATE FUNCTION reject_blob_orphan_delete_audit()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.event_json::jsonb ->> 'event' = 'blob_orphan_delete' THEN
                    RAISE EXCEPTION 'injected blob orphan audit failure';
                END IF;
                RETURN NEW;
            END;
            $$;
            CREATE TRIGGER reject_blob_orphan_delete_audit
            BEFORE INSERT ON audit_events
            FOR EACH ROW EXECUTE FUNCTION reject_blob_orphan_delete_audit();
            ",
        )
        .map_err(|error| error.to_string())
}

fn remove_audit_failure(dsn: &str, schema: &str) -> Result<(), String> {
    let mut client = schema_client(dsn, schema)?;
    client
        .batch_execute(
            "
            DROP TRIGGER reject_blob_orphan_delete_audit ON audit_events;
            DROP FUNCTION reject_blob_orphan_delete_audit();
            ",
        )
        .map_err(|error| error.to_string())
}

fn require_delete_audit(dsn: &str, schema: &str, blob_ref: &BlobRef) -> Result<(), String> {
    let mut client = schema_client(dsn, schema)?;
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM audit_events
             WHERE event_json::jsonb ->> 'event' = 'blob_orphan_delete'
               AND event_json::jsonb ->> 'blob_ref' = $1",
            &[&blob_ref.as_str()],
        )
        .map_err(|error| error.to_string())?
        .get(0);
    if count != 1 {
        return Err(format!(
            "eligible orphan has {count} committed delete audit records; expected 1"
        ));
    }
    Ok(())
}

fn schema_client(dsn: &str, schema: &str) -> Result<Client, String> {
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
