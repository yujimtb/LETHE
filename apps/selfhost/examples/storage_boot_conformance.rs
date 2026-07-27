use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lethe_core::domain::DataSpaceId;
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_selfhost::self_host::app::AppService;
use lethe_selfhost::self_host::config::{
    ApiTokenConfig, CorpusProjectionConfig, FreshnessConfig, JsonWebKeySet, McpOAuthConfig,
    OperationalLedgerConfig, OpsConfig, ResourceLimits, S3BlobConfig, S3TlsPolicy, SecretString,
    SelfHostConfig, StorageConfig, SupplementalConfig,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let (backend, root, storage) = match arguments.as_slice() {
        [_, mode, root] if mode == "sqlite" => {
            let root = PathBuf::from(root);
            let storage = StorageConfig::Sqlite {
                database_path: root.join("general.sqlite3"),
                blob_dir: root.join("general-blobs"),
                secret_encryption_key: [11; 32],
            };
            ("sqlite", root, storage)
        }
        [
            _,
            mode,
            root,
            dsn,
            schema,
            role,
            data_space_id,
            endpoint,
            bucket,
            access_key,
            secret_key,
        ] if mode == "postgres" => {
            let root = PathBuf::from(root);
            let storage = StorageConfig::Postgres {
                data_space_id: DataSpaceId::new(data_space_id),
                dsn: SecretString::new(dsn).map_err(|error| error.to_string())?,
                schema: schema.to_owned(),
                role: role.to_owned(),
                read_pool_size: 2,
                blobs: S3BlobConfig {
                    endpoint: endpoint.to_owned(),
                    region: "us-east-1".to_owned(),
                    bucket: bucket.to_owned(),
                    access_key: SecretString::new(access_key).map_err(|error| error.to_string())?,
                    secret_key: SecretString::new(secret_key).map_err(|error| error.to_string())?,
                    path_style: true,
                    tls_policy: S3TlsPolicy::TestHttp,
                    timeout_seconds: 3,
                    max_object_bytes: 4096,
                    orphan_min_age_seconds: 3,
                },
            };
            ("postgres", root, storage)
        }
        _ => {
            return Err(
                "usage: storage_boot_conformance sqlite <root> | postgres <root> <dsn> <schema> <role> <data-space-id> <endpoint> <bucket> <access-key> <secret-key>"
                    .to_owned(),
            );
        }
    };
    std::fs::create_dir_all(&root)
        .map_err(|error| format!("creating fixture root {}: {error}", root.display()))?;
    let config = fixture_config(&root, storage)?;
    let service = AppService::bootstrap(config).map_err(|error| error.to_string())?;
    let health = service.deep_health().map_err(|error| error.to_string())?;
    let storage = health
        .dependencies
        .iter()
        .find(|dependency| dependency.name == "storage")
        .ok_or_else(|| "deep health omitted storage dependency".to_owned())?;
    if storage.status != "ok" {
        return Err(format!(
            "{backend} storage deep health failed: {:?}",
            storage.detail
        ));
    }
    println!("selfhost_storage_boot={backend}:passed");
    Ok(())
}

fn fixture_config(root: &Path, storage: StorageConfig) -> Result<SelfHostConfig, String> {
    Ok(SelfHostConfig {
        bind_addr: "127.0.0.1:0".to_owned(),
        mcp_bind_addr: "127.0.0.1:0".to_owned(),
        mcp_oauth: McpOAuthConfig {
            resource_url: "https://pre-nas.invalid/mcp".to_owned(),
            protected_resource_metadata_url:
                "https://pre-nas.invalid/.well-known/oauth-protected-resource".to_owned(),
            issuer: "https://pre-nas.invalid/".to_owned(),
            audience: "lethe-pre-nas".to_owned(),
            jwks_path: root.join("unused-jwks.json"),
            jwks: JsonWebKeySet { keys: Vec::new() },
        },
        storage,
        operational_ledger: OperationalLedgerConfig::Sqlite {
            data_space_id: DataSpaceId::new("space:pre-nas-operational"),
            database_path: root.join("operational.sqlite3"),
            blob_dir: root.join("operational-blobs"),
            secret_encryption_key: [12; 32],
        },
        poll_interval: Duration::from_secs(300),
        routing_key_order: RoutingKeyOrder::YearMonthSourceContainerPublished,
        api_tokens: vec![ApiTokenConfig {
            token: SecretString::new("pre-nas-test-token").map_err(|error| error.to_string())?,
            scopes: vec!["*".to_owned()],
        }],
        resource_limits: ResourceLimits {
            max_blob_bytes: 4096,
            max_payload_bytes: 4096,
            max_sync_items: 100,
            max_concurrent_imports: 1,
            max_import_drafts: 100,
            max_page_size: 100,
            max_search_job_workers: 1,
            max_search_job_records: 100,
            max_source_export_scan_records: 1_000,
            max_leaf_observations: 1000,
            retention_days: 30,
        },
        corpus: CorpusProjectionConfig {
            mode: lethe_projection_corpus::CorpusMode::WorkspaceFiltered,
            index_dir: root.join("corpus-index"),
            writer_heap_bytes: 32 * 1024 * 1024,
            rebuild_page_size: 64,
        },
        freshness: FreshnessConfig {
            threshold_seconds: BTreeMap::from([("fixture".to_owned(), 3600)]),
        },
        ops: OpsConfig {
            backfill_nightly_budget_items: 100,
        },
        channels: Vec::new(),
        slack_sources: Vec::new(),
        google_sources: Vec::new(),
        slide_analysis_limit: None,
        slide_ai: None,
        supplemental: SupplementalConfig {
            reject_unregistered_kinds: true,
        },
    })
}
