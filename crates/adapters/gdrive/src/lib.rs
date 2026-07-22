//! Google Drive file adapter — allowlisted file snapshots.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use lethe_adapter_api::config::AdapterConfig;
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::heartbeat::heartbeat_draft;
use lethe_adapter_api::idempotency::{
    CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, canonical_json, identity_key,
};
use lethe_adapter_api::traits::{Cursor, FetchResult, ObservationDraft, RawData, SourceAdapter};
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};
use serde::{Deserialize, Serialize};

pub const WORKSPACE_SNAPSHOT_SCHEMA: &str = "schema:workspace-object-snapshot";
pub const WORKSPACE_SNAPSHOT_SCHEMA_VERSION: &str = "1.0.0";
pub const OBSERVER_ID: &str = "obs:gdrive-crawler";
pub const SOURCE_SYSTEM: &str = "sys:google-drive";
pub const DEFAULT_CRAWL_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveFile {
    pub file_id: String,
    pub revision_id: String,
    pub name: String,
    pub mime_type: String,
    pub modified_time: DateTime<Utc>,
    pub canonical_uri: String,
    #[serde(default)]
    pub parent_ids: Vec<String>,
    #[serde(default)]
    pub owner: Option<String>,
    pub sharing_level: SharingLevel,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SharingLevel {
    Private,
    SpecificUsers,
    Domain,
    AnyoneWithLink,
    Public,
}

#[derive(Debug, Clone)]
pub struct DriveCrawlConfig {
    pub allowed_folder_ids: Vec<String>,
    pub crawl_interval: Duration,
}

impl Default for DriveCrawlConfig {
    fn default() -> Self {
        Self {
            allowed_folder_ids: vec![],
            crawl_interval: DEFAULT_CRAWL_INTERVAL,
        }
    }
}

pub trait GoogleDriveClient {
    fn list_files(&self, folder_id: &str) -> Result<Vec<DriveFile>, AdapterError>;
    fn get_file(&self, file_id: &str) -> Result<DriveFile, AdapterError>;
    fn export_text(&self, file_id: &str) -> Result<Option<String>, AdapterError>;
}

#[derive(Debug, Default)]
pub struct FixtureGoogleDriveClient {
    pub files: HashMap<String, DriveFile>,
}

impl FixtureGoogleDriveClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_file(mut self, file: DriveFile) -> Self {
        self.files.insert(file.file_id.clone(), file);
        self
    }
}

impl GoogleDriveClient for FixtureGoogleDriveClient {
    fn list_files(&self, folder_id: &str) -> Result<Vec<DriveFile>, AdapterError> {
        Ok(self
            .files
            .values()
            .filter(|file| file.parent_ids.iter().any(|parent| parent == folder_id))
            .cloned()
            .collect())
    }

    fn get_file(&self, file_id: &str) -> Result<DriveFile, AdapterError> {
        self.files
            .get(file_id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("drive file {file_id} not found")))
    }

    fn export_text(&self, file_id: &str) -> Result<Option<String>, AdapterError> {
        Ok(self.get_file(file_id)?.text)
    }
}

pub struct GoogleDriveAdapter<C: GoogleDriveClient> {
    pub client: C,
    pub config: AdapterConfig,
    pub crawl: DriveCrawlConfig,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl<C: GoogleDriveClient> GoogleDriveAdapter<C> {
    pub fn new(client: C, config: AdapterConfig, crawl: DriveCrawlConfig) -> Self {
        Self {
            client,
            config,
            crawl,
            last_successful_capture: None,
        }
    }

    pub fn map_file(&self, file: &DriveFile) -> ObservationDraft {
        let object_id = format!("drive-file:{}:revision:{}", file.file_id, file.revision_id);
        let payload = serde_json::json!({
            "title": file.name,
            "artifact": {
                "provider": "google",
                "service": "drive",
                "objectType": file.mime_type,
                "sourceObjectId": file.file_id,
                "canonicalUri": file.canonical_uri,
                "containerId": file.parent_ids.first(),
            },
            "revision": {
                "sourceRevisionId": file.revision_id,
                "sourceModifiedAt": file.modified_time,
                "captureMode": "snapshot",
            },
            "native": {
                "encoding": "inline-text",
                "text": file.text,
            },
            "relations": {
                "owner": file.owner,
            },
            "metadata": {
                "sharingLevel": file.sharing_level,
                "parentIds": file.parent_ids,
            },
        });
        let canonical_json = canonical_json(&serde_json::json!({
            "file_id": file.file_id,
            "revision_id": file.revision_id,
            "text": file.text,
            "sharing_level": file.sharing_level,
        }));
        ObservationDraft {
            schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
            schema_version: SemVer::new(WORKSPACE_SNAPSHOT_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new(format!("document:gdrive:{}", file.file_id)),
            target: None,
            payload,
            attachments: vec![],
            published: file.modified_time,
            idempotency_key: identity_key("google-drive", &object_id, &canonical_json),
            client_ref: None,
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
                OBJECT_ID_META_KEY: object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
                "source_container": "google-drive",
                "sharing_level": file.sharing_level,
                "parent_ids": file.parent_ids,
            }),
        }
    }

    fn crawl_folder(&self, folder_id: &str, items: &mut Vec<RawData>) -> Result<(), AdapterError> {
        for file in self.client.list_files(folder_id)? {
            if file.mime_type == "application/vnd.google-apps.folder" {
                self.crawl_folder(&file.file_id, items)?;
            } else {
                items.push(RawData {
                    data: serde_json::to_value(file).unwrap_or_default(),
                    blobs: vec![],
                });
            }
        }
        Ok(())
    }
}

impl<C: GoogleDriveClient> SourceAdapter for GoogleDriveAdapter<C> {
    fn fetch_incremental(&self, _cursor: Option<&Cursor>) -> FetchResult {
        let mut items = Vec::new();
        for folder in &self.crawl.allowed_folder_ids {
            match self.crawl_folder(folder, &mut items) {
                Ok(()) => {}
                Err(err) => return FetchResult::Error(err),
            }
        }
        FetchResult::Ok {
            items,
            next_cursor: Some(Cursor {
                value: Utc::now().to_rfc3339(),
                updated_at: Utc::now(),
            }),
            has_more: false,
        }
    }

    fn fetch_snapshot(&self, target_id: &str) -> FetchResult {
        match self.client.get_file(target_id) {
            Ok(file) => FetchResult::Ok {
                items: vec![RawData {
                    data: serde_json::to_value(file).unwrap_or_default(),
                    blobs: vec![],
                }],
                next_cursor: None,
                has_more: false,
            },
            Err(err) => FetchResult::Error(err),
        }
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        let file = serde_json::from_value::<DriveFile>(raw.data.clone()).map_err(|err| {
            AdapterError::MalformedResponse {
                message: format!("Google Drive raw data is not a file: {err}"),
            }
        })?;
        Ok(vec![self.map_file(&file)])
    }

    fn heartbeat(&self) -> ObservationDraft {
        heartbeat_draft(
            &ObserverRef::new(OBSERVER_ID),
            &SourceSystemRef::new(SOURCE_SYSTEM),
            Utc::now(),
            0,
            self.last_successful_capture,
        )
    }

    fn observer_ref(&self) -> &ObserverRef {
        &self.config.observer_id
    }

    fn source_system_ref(&self) -> &SourceSystemRef {
        &self.config.source_system_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_adapter_api::config::{BackoffStrategy, RateLimitConfig, RetryConfig, SchemaBinding};
    use lethe_adapter_api::traits::RawData;
    use std::time::Duration;

    fn config() -> AdapterConfig {
        AdapterConfig {
            observer_id: ObserverRef::new(OBSERVER_ID),
            source_system_id: SourceSystemRef::new(SOURCE_SYSTEM),
            adapter_version: SemVer::new("1.0.0"),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            schemas: vec![SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA)],
            schema_bindings: vec![SchemaBinding {
                schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
                versions: ">=1.0.0 <2.0.0".into(),
            }],
            poll_interval: DEFAULT_CRAWL_INTERVAL,
            heartbeat_interval: Duration::from_secs(60),
            rate_limit: RateLimitConfig {
                requests_per_second: 10,
                burst: 5,
            },
            retry: RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential,
                max_wait: Duration::from_secs(60),
            },
            credential_ref: "secret:google".into(),
        }
    }

    #[test]
    fn drive_file_records_text_and_sharing_level() {
        let file = DriveFile {
            file_id: "file1".into(),
            revision_id: "rev1".into(),
            name: "Policy".into(),
            mime_type: "text/plain".into(),
            modified_time: Utc::now(),
            canonical_uri: "https://drive.google.com/file/d/file1".into(),
            parent_ids: vec!["folder1".into()],
            owner: Some("owner@example.com".into()),
            sharing_level: SharingLevel::Domain,
            text: Some("hello".into()),
        };
        let adapter = GoogleDriveAdapter::new(
            FixtureGoogleDriveClient::new(),
            config(),
            DriveCrawlConfig {
                allowed_folder_ids: vec!["folder1".into()],
                crawl_interval: DEFAULT_CRAWL_INTERVAL,
            },
        );
        let raw = RawData {
            data: serde_json::to_value(&file).unwrap(),
            blobs: vec![],
        };
        let draft = adapter.to_observations(&raw).unwrap().remove(0);
        assert_eq!(draft.payload["artifact"]["service"], "drive");
        assert_eq!(draft.payload["metadata"]["sharingLevel"], "domain");
        assert_eq!(draft.payload["native"]["text"], "hello");
    }
}
