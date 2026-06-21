//! M02 Registry — Observation Schema definitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use lethe_core::domain::{EntityTypeRef, ObserverRef, SchemaRef, SemVer};

/// An Observation payload schema (JSON Schema based).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationSchema {
    /// `schema:{name}` format.
    pub id: SchemaRef,
    pub name: String,
    pub version: SemVer,
    /// Which entity type this schema's subject must be.
    pub subject_type: EntityTypeRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<EntityTypeRef>,
    /// JSON Schema document for the payload.
    pub payload_schema: serde_json::Value,
    /// Adapters that are known to emit this schema.
    #[serde(default)]
    pub source_contracts: Vec<SchemaSourceContract>,
    #[serde(default)]
    pub attachment_config: Option<AttachmentConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSourceContract {
    pub observer_id: ObserverRef,
    pub adapter_version: SemVer,
    /// SemVer range string, e.g. ">=1.0.0 <2.0.0"
    pub compatible_range: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentConfig {
    pub required: bool,
    #[serde(default)]
    pub accepted_types: Vec<String>,
}

/// A frozen snapshot of a schema at a specific version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub schema_id: SchemaRef,
    pub version: SemVer,
    pub payload_schema: serde_json::Value,
    pub created_at: DateTime<Utc>,
}
