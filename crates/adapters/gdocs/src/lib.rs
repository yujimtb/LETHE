//! Google Docs adapter — workspace-object-snapshot mapper.

use std::collections::HashMap;

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
pub const OBSERVER_ID: &str = "obs:gdocs-crawler";
pub const SOURCE_SYSTEM: &str = "sys:google-docs";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleDoc {
    pub document_id: String,
    pub revision_id: String,
    pub title: String,
    #[serde(default)]
    pub body_text: String,
    pub modified_time: DateTime<Utc>,
    pub canonical_uri: String,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub headings: Vec<DocHeading>,
    #[serde(default)]
    pub links: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocHeading {
    pub level: u8,
    pub title: String,
    pub text: String,
    #[serde(default)]
    pub anchor: Option<String>,
}

pub trait GoogleDocsClient {
    fn get_document(&self, document_id: &str) -> Result<GoogleDoc, AdapterError>;
    fn list_revisions(&self, document_id: &str) -> Result<Vec<String>, AdapterError>;
}

#[derive(Debug, Default)]
pub struct FixtureGoogleDocsClient {
    pub docs: HashMap<String, GoogleDoc>,
}

impl FixtureGoogleDocsClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_document(mut self, doc: GoogleDoc) -> Self {
        self.docs.insert(doc.document_id.clone(), doc);
        self
    }
}

impl GoogleDocsClient for FixtureGoogleDocsClient {
    fn get_document(&self, document_id: &str) -> Result<GoogleDoc, AdapterError> {
        self.docs
            .get(document_id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("document {document_id} not found")))
    }

    fn list_revisions(&self, document_id: &str) -> Result<Vec<String>, AdapterError> {
        Ok(vec![self.get_document(document_id)?.revision_id])
    }
}

pub struct GoogleDocsAdapter<C: GoogleDocsClient> {
    pub client: C,
    pub config: AdapterConfig,
    pub cursors: HashMap<String, String>,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl<C: GoogleDocsClient> GoogleDocsAdapter<C> {
    pub fn new(client: C, config: AdapterConfig) -> Self {
        Self {
            client,
            config,
            cursors: HashMap::new(),
            last_successful_capture: None,
        }
    }

    pub fn map_document(&self, doc: &GoogleDoc) -> ObservationDraft {
        let object_id = format!("document:{}:revision:{}", doc.document_id, doc.revision_id);
        let chunks = if doc.headings.is_empty() {
            vec![serde_json::json!({
                "heading": null,
                "level": null,
                "text": doc.body_text,
                "anchor": null,
            })]
        } else {
            doc.headings
                .iter()
                .map(|heading| {
                    serde_json::json!({
                        "heading": heading.title,
                        "level": heading.level,
                        "text": heading.text,
                        "anchor": heading.anchor,
                    })
                })
                .collect()
        };
        let payload = serde_json::json!({
            "title": doc.title,
            "artifact": {
                "provider": "google",
                "service": "docs",
                "objectType": "document",
                "sourceObjectId": doc.document_id,
                "canonicalUri": doc.canonical_uri,
            },
            "revision": {
                "sourceRevisionId": doc.revision_id,
                "sourceModifiedAt": doc.modified_time,
                "captureMode": "snapshot",
            },
            "native": {
                "encoding": "inline-json",
                "chunks": chunks,
                "links": doc.links,
            },
            "relations": {
                "owner": doc.owner,
            },
        });
        let canonical_tuple = serde_json::json!({
            "document_id": doc.document_id,
            "revision_id": doc.revision_id,
            "chunks": chunks,
        });
        let canonical_json = canonical_json(&canonical_tuple);
        ObservationDraft {
            schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
            schema_version: SemVer::new(WORKSPACE_SNAPSHOT_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new(format!("document:gdocs:{}", doc.document_id)),
            target: None,
            payload,
            attachments: vec![],
            published: doc.modified_time,
            idempotency_key: identity_key("google-docs", &object_id, &canonical_json),
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
                OBJECT_ID_META_KEY: object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
                "source_container": "google-docs",
            }),
        }
    }

    pub fn update_cursor(&mut self, document_id: &str, revision_id: &str) {
        self.cursors
            .insert(document_id.to_owned(), revision_id.to_owned());
    }
}

impl<C: GoogleDocsClient> SourceAdapter for GoogleDocsAdapter<C> {
    fn fetch_incremental(&self, _cursor: Option<&Cursor>) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "GoogleDocsAdapter::fetch_incremental requires explicit document IDs".into(),
        ))
    }

    fn fetch_snapshot(&self, target_id: &str) -> FetchResult {
        match self.client.get_document(target_id) {
            Ok(doc) => FetchResult::Ok {
                items: vec![RawData {
                    data: serde_json::to_value(doc).unwrap_or_default(),
                    blobs: vec![],
                }],
                next_cursor: None,
                has_more: false,
            },
            Err(err) => FetchResult::Error(err),
        }
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        let doc = serde_json::from_value::<GoogleDoc>(raw.data.clone()).map_err(|err| {
            AdapterError::MalformedResponse {
                message: format!("Google Docs raw data is not a document: {err}"),
            }
        })?;
        Ok(vec![self.map_document(&doc)])
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
            poll_interval: Duration::from_secs(86400),
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
    fn document_maps_to_workspace_snapshot_with_chunks() {
        let adapter = GoogleDocsAdapter::new(FixtureGoogleDocsClient::new(), config());
        let doc = GoogleDoc {
            document_id: "doc1".into(),
            revision_id: "rev1".into(),
            title: "Handbook".into(),
            modified_time: Utc::now(),
            canonical_uri: "https://docs.google.com/document/d/doc1".into(),
            body_text: "Welcome".into(),
            owner: Some("owner@example.com".into()),
            headings: vec![DocHeading {
                level: 1,
                title: "Intro".into(),
                text: "Welcome".into(),
                anchor: Some("h.1".into()),
            }],
            links: vec![],
        };
        let draft = adapter.map_document(&doc);
        assert_eq!(draft.payload["artifact"]["service"], "docs");
        assert_eq!(draft.payload["native"]["chunks"][0]["heading"], "Intro");
        assert!(draft.idempotency_key.as_str().starts_with("google-docs:"));
    }
}
