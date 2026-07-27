use std::time::Duration;

use lethe_core::domain::BlobRef;
use lethe_storage_api::{BlobStore, conformance};
use lethe_storage_postgres::{S3BlobStore, S3BlobStoreConfig, S3TransportPolicy};

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
        endpoint,
        region,
        bucket,
        access_key,
        secret_key,
        corrupt_ref,
    ] = arguments.as_slice()
    else {
        return Err(
            "usage: s3_conformance <endpoint> <region> <bucket> <access-key> <secret-key> <corrupt-blob-ref>"
                .to_owned(),
        );
    };
    let config = S3BlobStoreConfig {
        endpoint: endpoint.to_owned(),
        region: region.to_owned(),
        bucket: bucket.to_owned(),
        access_key: access_key.to_owned(),
        secret_key: secret_key.to_owned(),
        path_style: true,
        transport_policy: S3TransportPolicy::TestHttp,
        timeout: Duration::from_secs(3),
        max_object_bytes: 1024,
        orphan_min_age: Duration::from_secs(1),
    };
    let store = S3BlobStore::connect(config.clone()).map_err(|error| error.to_string())?;
    conformance::blob_store_round_trip(&store);
    verify_batch_preflight(&store)?;
    verify_missing(&store)?;
    verify_corrupt_object(&store, corrupt_ref)?;
    verify_timeout(config)?;
    println!("s3_conformance=passed");
    Ok(())
}

fn verify_corrupt_object(store: &S3BlobStore, corrupt_ref: &str) -> Result<(), String> {
    let expected = BlobRef::new(corrupt_ref);
    if S3BlobStore::object_key(&expected).is_err() {
        return Err("corrupt object fixture blob reference is invalid".to_owned());
    }
    if store.get_blob(&expected).is_ok() || store.put_blob(b"declared-content", 1024).is_ok() {
        return Err("S3 digest-key content mismatch was accepted".to_owned());
    }
    Ok(())
}

fn verify_batch_preflight(store: &S3BlobStore) -> Result<(), String> {
    let before = store
        .put_blob(b"preflight-existing", 1024)
        .map_err(|error| error.to_string())?;
    if store
        .put_blobs(&[b"must-not-be-written", b"too-large"], 3)
        .is_ok()
    {
        return Err("oversized S3 batch was accepted".to_owned());
    }
    if store
        .get_blob(&before)
        .map_err(|error| error.to_string())?
        .as_deref()
        != Some(b"preflight-existing")
    {
        return Err("failed S3 batch damaged an existing object".to_owned());
    }
    Ok(())
}

fn verify_missing(store: &S3BlobStore) -> Result<(), String> {
    let missing = BlobRef::new(format!("blob:sha256:{}", "0".repeat(64)));
    if store
        .get_blob(&missing)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("missing S3 object returned bytes".to_owned());
    }
    Ok(())
}

fn verify_timeout(mut config: S3BlobStoreConfig) -> Result<(), String> {
    config.endpoint = "http://127.0.0.1:59999".to_owned();
    config.timeout = Duration::from_millis(100);
    if S3BlobStore::connect(config).is_ok() {
        return Err("unreachable S3 endpoint was admitted".to_owned());
    }
    Ok(())
}
