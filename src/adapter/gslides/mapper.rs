//! M11 — Google Slides → Observation mapper + GoogleSlidesAdapter implementation
//!
//! Produces `schema:workspace-object-snapshot` Observations.
//! Capture model = snapshot (mutable+multimodal source).
//! Authority model = source-authoritative.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::adapter::config::AdapterConfig;
use crate::adapter::error::AdapterError;
use crate::adapter::heartbeat::heartbeat_draft;
use crate::adapter::idempotency::{
    CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, canonical_json, identity_key,
};
use crate::adapter::traits::*;
use crate::domain::{
    AuthorityModel, BlobRef, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer,
    SourceSystemRef,
};

use super::client::*;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const WORKSPACE_SNAPSHOT_SCHEMA: &str = "schema:workspace-object-snapshot";
pub const WORKSPACE_SNAPSHOT_SCHEMA_VERSION: &str = "1.0.0";

const OBSERVER_ID: &str = "obs:gslides-crawler";
const SOURCE_SYSTEM: &str = "sys:google-slides";

// ---------------------------------------------------------------------------
// GoogleSlidesAdapter
// ---------------------------------------------------------------------------

pub struct GoogleSlidesAdapter<C: GoogleSlidesClient> {
    pub client: C,
    pub config: AdapterConfig,
    /// Per-presentation cursor: presentation_id → last known revision_id.
    pub cursors: HashMap<String, String>,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl<C: GoogleSlidesClient> GoogleSlidesAdapter<C> {
    pub fn new(client: C, config: AdapterConfig) -> Self {
        Self {
            client,
            config,
            cursors: HashMap::new(),
            last_successful_capture: None,
        }
    }

    /// Map a revision + presentation native data into an ObservationDraft.
    ///
    /// `native_blob_ref`: if the native JSON was stored as a blob, reference it here.
    /// `rendered_blob_refs`: rendered slide images stored as blobs.
    pub fn map_revision(
        &self,
        revision: &SlideRevision,
        meta: &PresentationMeta,
        native_blob_ref: Option<BlobRef>,
        rendered_blob_refs: Vec<BlobRef>,
    ) -> ObservationDraft {
        let object_id = format!(
            "presentation:{}:revision:{}",
            revision.presentation_id, revision.revision_id
        );

        let subject = EntityRef::new(format!("document:gslides:{}", revision.presentation_id));

        let capture_mode = if rendered_blob_refs.is_empty() {
            "snapshot"
        } else {
            "hybrid"
        };

        let native_encoding = if native_blob_ref.is_some() {
            "blob-ref"
        } else {
            "inline-json"
        };

        let payload = serde_json::json!({
            "title": meta.title,
            "artifact": {
                "provider": "google",
                "service": "slides",
                "objectType": "presentation",
                "sourceObjectId": revision.presentation_id,
                "containerId": meta.container_id,
                "canonicalUri": meta.canonical_uri,
            },
            "revision": {
                "sourceRevisionId": revision.revision_id,
                "sourceModifiedAt": revision.modified_time,
                "captureMode": capture_mode,
            },
            "native": {
                "encoding": native_encoding,
                "blobRef": native_blob_ref.as_ref().map(|b| b.as_str().to_string()),
            },
            "relations": {
                "owner": meta.owner,
                "editors": meta.editors,
                "viewers": meta.viewers,
            },
        });

        let mut attachments = rendered_blob_refs;
        if let Some(ref nb) = native_blob_ref {
            attachments.push(nb.clone());
        }
        let attachment_sha256 = attachments
            .iter()
            .filter_map(|blob_ref| blob_ref.as_str().strip_prefix("blob:sha256:"))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let canonical_tuple = serde_json::json!({
            "presentation_id": revision.presentation_id,
            "revision_id": revision.revision_id,
            "modified_time": revision.modified_time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "attachment_sha256": attachment_sha256,
        });
        let canonical_json = canonical_json(&canonical_tuple);
        let idem_key = identity_key("google-slides", &object_id, &canonical_json);

        ObservationDraft {
            schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
            schema_version: SemVer::new(WORKSPACE_SNAPSHOT_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject,
            target: None,
            payload,
            attachments,
            published: revision.modified_time,
            idempotency_key: idem_key,
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
                OBJECT_ID_META_KEY: object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
            }),
        }
    }

    /// Update the per-presentation cursor.
    pub fn update_cursor(&mut self, presentation_id: &str, revision_id: &str) {
        self.cursors
            .insert(presentation_id.to_string(), revision_id.to_string());
    }

    pub fn get_cursor(&self, presentation_id: &str) -> Option<&str> {
        self.cursors.get(presentation_id).map(String::as_str)
    }
}

impl<C: GoogleSlidesClient> SourceAdapter for GoogleSlidesAdapter<C> {
    fn fetch_incremental(&self, _cursor: Option<&Cursor>) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "GoogleSlidesAdapter::fetch_incremental requires an explicit presentation ID; use GoogleSlidesClient::list_revisions".into(),
        ))
    }

    fn fetch_snapshot(&self, target_id: &str) -> FetchResult {
        match self.client.get_presentation(target_id) {
            Ok(pres) => FetchResult::Ok {
                items: vec![RawData {
                    data: serde_json::to_value(&pres).unwrap_or_default(),
                    blobs: vec![],
                }],
                next_cursor: None,
                has_more: false,
            },
            Err(e) => FetchResult::Error(e),
        }
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        // For revision-based data, we need meta context to build a full observation.
        // This trait method is used for simple transformations; the full pipeline
        // uses map_revision directly.
        if let Ok(rev) = serde_json::from_value::<SlideRevision>(raw.data.clone()) {
            let placeholder_meta = PresentationMeta {
                presentation_id: rev.presentation_id.clone(),
                title: String::new(),
                container_id: None,
                canonical_uri: format!(
                    "https://docs.google.com/presentation/d/{}",
                    rev.presentation_id
                ),
                owner: None,
                editors: vec![],
                viewers: vec![],
            };
            Ok(vec![self.map_revision(
                &rev,
                &placeholder_meta,
                None,
                vec![],
            )])
        } else {
            Err(AdapterError::MalformedResponse {
                message: "Google Slides raw data is not a SlideRevision".to_string(),
            })
        }
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::config::*;
    use crate::adapter::error::AdapterError;
    use std::time::Duration;

    fn test_config() -> AdapterConfig {
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
            poll_interval: Duration::from_secs(600),
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
            credential_ref: "secret:google-sa".into(),
        }
    }

    fn sample_revision() -> SlideRevision {
        SlideRevision {
            presentation_id: "pres123".into(),
            revision_id: "rev456".into(),
            modified_time: chrono::DateTime::parse_from_rfc3339("2026-05-01T10:00:00Z")
                .unwrap()
                .to_utc(),
            last_modifying_user: Some("editor@example.com".into()),
        }
    }

    fn sample_meta() -> PresentationMeta {
        PresentationMeta {
            presentation_id: "pres123".into(),
            title: "寮紹介スライド".into(),
            container_id: Some("folder789".into()),
            canonical_uri: "https://docs.google.com/presentation/d/pres123".into(),
            owner: Some("owner@example.com".into()),
            editors: vec!["editor@example.com".into()],
            viewers: vec!["viewer@example.com".into()],
        }
    }

    fn sample_presentation() -> PresentationNative {
        PresentationNative {
            presentation_id: "pres123".into(),
            title: "寮紹介スライド".into(),
            locale: Some("ja".into()),
            slides: vec![SlideNative {
                object_id: "slide1".into(),
                page_elements: vec![serde_json::json!({"type": "text", "content": "Hello"})],
            }],
            page_size: Some(PageSize {
                width_emu: 9144000,
                height_emu: 5143500,
            }),
        }
    }

    #[test]
    fn map_revision_snapshot_only() {
        let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        let rev = sample_revision();
        let meta = sample_meta();

        let draft = adapter.map_revision(&rev, &meta, None, vec![]);
        assert_eq!(draft.schema.as_str(), WORKSPACE_SNAPSHOT_SCHEMA);
        assert!(
            draft
                .idempotency_key
                .as_str()
                .starts_with("google-slides:presentation:pres123:revision:rev456:")
        );
        assert!(draft.meta[CANONICAL_JSON_META_KEY].is_string());
        assert_eq!(draft.subject.as_str(), "document:gslides:pres123");
        assert_eq!(draft.authority_model, AuthorityModel::SourceAuthoritative);
        assert_eq!(draft.capture_model, CaptureModel::Snapshot);
        assert_eq!(draft.payload["revision"]["captureMode"], "snapshot");
        assert_eq!(draft.payload["artifact"]["provider"], "google");
        assert_eq!(draft.payload["artifact"]["service"], "slides");
        assert_eq!(draft.payload["title"], "寮紹介スライド");
    }

    #[test]
    fn map_revision_hybrid_with_blobs() {
        let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        let rev = sample_revision();
        let meta = sample_meta();
        let native_blob = BlobRef::new("blob:sha256:native111");
        let rendered_blob = BlobRef::new("blob:sha256:rendered222");

        let draft =
            adapter.map_revision(&rev, &meta, Some(native_blob.clone()), vec![rendered_blob]);

        assert_eq!(draft.payload["revision"]["captureMode"], "hybrid");
        assert_eq!(draft.payload["native"]["encoding"], "blob-ref");
        assert_eq!(draft.attachments.len(), 2);
    }

    #[test]
    fn same_revision_same_idempotency_key() {
        let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        let rev = sample_revision();
        let meta = sample_meta();

        let d1 = adapter.map_revision(&rev, &meta, None, vec![]);
        let d2 = adapter.map_revision(&rev, &meta, None, vec![]);
        assert_eq!(d1.idempotency_key, d2.idempotency_key);
    }

    #[test]
    fn revision_contains_relations() {
        let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        let rev = sample_revision();
        let meta = sample_meta();

        let draft = adapter.map_revision(&rev, &meta, None, vec![]);
        assert_eq!(
            draft.payload["relations"]["editors"][0],
            "editor@example.com"
        );
        assert_eq!(draft.payload["relations"]["owner"], "owner@example.com");
    }

    #[test]
    fn fetch_incremental_requires_explicit_presentation_context() {
        let revs = vec![sample_revision()];
        let client = FixtureGoogleSlidesClient::new().with_revisions(revs);
        let adapter = GoogleSlidesAdapter::new(client, test_config());

        match adapter.fetch_incremental(None) {
            FetchResult::Error(AdapterError::Other(message)) => {
                assert!(message.contains("explicit presentation ID"));
            }
            FetchResult::Ok { .. } => panic!("expected unsupported fetch_incremental error"),
            FetchResult::Error(e) => panic!("unexpected error: {e}"),
        }
    }

    #[test]
    fn fetch_snapshot_via_fixture() {
        let pres = sample_presentation();
        let client = FixtureGoogleSlidesClient::new().with_presentation(pres);
        let adapter = GoogleSlidesAdapter::new(client, test_config());

        match adapter.fetch_snapshot("pres123") {
            FetchResult::Ok { items, .. } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].data["title"], "寮紹介スライド");
            }
            FetchResult::Error(e) => panic!("unexpected error: {e}"),
        }
    }

    #[test]
    fn to_observations_from_revision_raw() {
        let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        let raw = RawData {
            data: serde_json::to_value(&sample_revision()).unwrap(),
            blobs: vec![],
        };
        let drafts = adapter.to_observations(&raw).unwrap();
        assert_eq!(drafts.len(), 1);
        assert_eq!(drafts[0].schema.as_str(), WORKSPACE_SNAPSHOT_SCHEMA);
    }

    #[test]
    fn heartbeat_generated() {
        let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        let hb = adapter.heartbeat();
        assert_eq!(hb.schema.as_str(), "schema:observer-heartbeat");
        assert_eq!(hb.observer.as_str(), OBSERVER_ID);
    }

    #[test]
    fn cursor_management() {
        let mut adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        assert!(adapter.get_cursor("pres123").is_none());

        adapter.update_cursor("pres123", "rev456");
        assert_eq!(adapter.get_cursor("pres123"), Some("rev456"));

        adapter.update_cursor("pres123", "rev789");
        assert_eq!(adapter.get_cursor("pres123"), Some("rev789"));
    }

    #[test]
    fn adapter_metadata_in_observations() {
        let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), test_config());
        let draft = adapter.map_revision(&sample_revision(), &sample_meta(), None, vec![]);
        assert_eq!(draft.meta["sourceAdapterVersion"], "1.0.0");
    }
}
