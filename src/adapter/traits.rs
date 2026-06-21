//! M09 — SourceAdapter trait and core protocol types

use crate::domain::{AuthorityModel, CaptureModel};
use crate::domain::{
    BlobRef, EntityRef, IdempotencyKey, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::error::AdapterError;

/// Opaque cursor that an adapter persists between incremental fetches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    /// Adapter-specific opaque value (e.g. Slack oldest-ts, GSlides revision id).
    pub value: String,
    /// When this cursor was produced.
    pub updated_at: DateTime<Utc>,
}

/// A draft observation produced by the adapter's `to_observations` step.
/// This is the pre-ID form that the adapter hands to the ingestion gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationDraft {
    pub schema: SchemaRef,
    pub schema_version: SemVer,
    pub observer: ObserverRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_system: Option<SourceSystemRef>,
    pub authority_model: AuthorityModel,
    pub capture_model: CaptureModel,
    pub subject: EntityRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<EntityRef>,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub attachments: Vec<BlobRef>,
    pub published: DateTime<Utc>,
    pub idempotency_key: IdempotencyKey,
    #[serde(default)]
    pub meta: serde_json::Value,
}

/// Raw data fetched from a source system before transformation.
#[derive(Debug, Clone)]
pub struct RawData {
    pub data: serde_json::Value,
    /// Optional binary attachments to upload.
    pub blobs: Vec<(String, Vec<u8>)>,
}

/// Result of an incremental or snapshot fetch.
#[derive(Debug, Clone)]
pub enum FetchResult {
    Ok {
        items: Vec<RawData>,
        next_cursor: Option<Cursor>,
        has_more: bool,
    },
    Error(AdapterError),
}

/// The core source-adapter protocol.
///
/// Adapters implement pure mapping (`to_observations`) separately from
/// IO (`fetch_incremental`, `fetch_snapshot`) so that the mapper can be
/// tested with fixtures alone.
pub trait SourceAdapter {
    /// Fetch delta since `cursor`. Returns raw data + next cursor.
    fn fetch_incremental(&self, cursor: Option<&Cursor>) -> FetchResult;

    /// Fetch a specific object's latest state (snapshot).
    fn fetch_snapshot(&self, target_id: &str) -> FetchResult;

    /// Pure mapping: transform raw source data into Observation drafts.
    ///
    /// Malformed source data must be surfaced explicitly; adapters must not
    /// silently drop records or synthesize replacement values.
    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError>;

    /// Generate a heartbeat Observation draft.
    fn heartbeat(&self) -> ObservationDraft;

    /// Return the adapter's observer ref.
    fn observer_ref(&self) -> &ObserverRef;

    /// Return the adapter's source system ref.
    fn source_system_ref(&self) -> &SourceSystemRef;
}
