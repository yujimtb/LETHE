//! M01 Domain Kernel — Observation record

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::types::{AuthorityModel, CaptureModel};
use super::values::*;

/// The canonical capture record.
///
/// An Observation is the **pre-interpretation capture** of something an
/// observer noticed.  It must never be mutated once ingested (Append-Only Law).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub id: ObservationId,
    pub schema: SchemaRef,
    pub schema_version: SemVer,
    pub observer: ObserverRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_system: Option<SourceSystemRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<EntityRef>,
    pub authority_model: AuthorityModel,
    pub capture_model: CaptureModel,
    /// PRIMARY: what this observation is about.
    pub subject: EntityRef,
    /// SECONDARY: an optional related entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<EntityRef>,
    /// Schema-validated payload (opaque JSON).
    pub payload: serde_json::Value,
    #[serde(default)]
    pub attachments: Vec<BlobRef>,
    /// Event time — offset-aware ISO 8601.
    pub published: DateTime<Utc>,
    /// System ingestion time (UTC).
    pub recorded_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consent: Option<ConsentRef>,
    pub idempotency_key: IdempotencyKey,
    #[serde(default)]
    pub meta: serde_json::Value,
}

impl Observation {
    /// Generate a new time-sortable observation id (UUID v7).
    pub fn new_id() -> ObservationId {
        ObservationId(Uuid::now_v7().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample_observation() -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("message:slack:C01-123"),
            target: None,
            payload: serde_json::json!({"text": "hello"}),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("slack:C01:123"),
            meta: serde_json::json!({}),
        }
    }

    #[test]
    fn observation_serializes_to_json() {
        let obs = sample_observation();
        let json = serde_json::to_string_pretty(&obs).unwrap();
        assert!(json.contains("slack-message"));
        // Deserialize back
        let back: Observation = serde_json::from_str(&json).unwrap();
        assert_eq!(back.schema, obs.schema);
    }

    #[test]
    fn observation_id_is_uuid_v7() {
        let id = Observation::new_id();
        let parsed = Uuid::parse_str(id.as_str()).unwrap();
        assert_eq!(parsed.get_version(), Some(uuid::Version::SortRand));
    }
}
