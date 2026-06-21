//! M01 Domain Kernel — Supplemental Record type

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::types::Mutability;
use super::values::*;

/// A non-canonical but reusable derivation stored alongside the Lake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupplementalRecord {
    pub id: SupplementalId,
    /// e.g. "transcript", "ocr-text", "embedding", …
    pub kind: String,
    pub derived_from: InputAnchorSet,
    pub payload: serde_json::Value,
    pub created_by: ActorRef,
    pub created_at: DateTime<Utc>,
    pub mutability: Mutability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consent_metadata: Option<ConsentMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage: Option<LineageRef>,
}

/// The set of canonical inputs this derivation was produced from.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InputAnchorSet {
    #[serde(default)]
    pub observations: Vec<ObservationId>,
    #[serde(default)]
    pub blobs: Vec<BlobRef>,
    #[serde(default)]
    pub supplementals: Vec<SupplementalId>,
}

/// Consent-related metadata attached to a supplemental record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsentMetadata {
    pub referenced_observation_id: ObservationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retracted_at: Option<DateTime<Utc>>,
    pub opt_out_strategy: OptOutStrategy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opt_out_effective_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptOutStrategy {
    Drop,
    Anonymize,
    Pseudonymize,
}
