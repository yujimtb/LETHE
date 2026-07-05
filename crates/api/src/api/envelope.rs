//! M14: Response Envelope — all API responses carry projection metadata.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use lethe_core::domain::{ProjectionRef, ReadMode, SemVer};

/// Projection metadata attached to every API response (M14 §4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionMetadata {
    pub projection_id: ProjectionRef,
    pub version: SemVer,
    pub built_at: DateTime<Utc>,
    pub read_mode: ReadMode,
    pub stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage_ref: Option<String>,
}

/// Standard response envelope (M14 §4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseEnvelope<T: Serialize> {
    pub data: T,
    pub projection_metadata: ProjectionMetadata,
}

/// Error response body (M14 §7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<u32>,
}

impl ErrorResponse {
    pub fn bad_request(detail: &str) -> Self {
        Self {
            error: "bad_request".into(),
            detail: Some(detail.into()),
            details: None,
            retry_after: None,
        }
    }

    pub fn internal_server_error(detail: &str) -> Self {
        Self {
            error: "internal_server_error".into(),
            detail: Some(detail.into()),
            details: None,
            retry_after: None,
        }
    }

    pub fn unauthorized() -> Self {
        Self {
            error: "unauthorized".into(),
            detail: None,
            details: None,
            retry_after: None,
        }
    }

    pub fn forbidden(detail: &str) -> Self {
        Self {
            error: "forbidden".into(),
            detail: Some(detail.into()),
            details: None,
            retry_after: None,
        }
    }

    pub fn not_found() -> Self {
        Self {
            error: "not_found".into(),
            detail: None,
            details: None,
            retry_after: None,
        }
    }

    pub fn service_unavailable(retry_after: u32) -> Self {
        Self {
            error: "service_unavailable".into(),
            detail: None,
            details: None,
            retry_after: Some(retry_after),
        }
    }

    pub fn projection_stale(detail: &str, retry_after: u32) -> Self {
        Self {
            error: "projection_stale".into(),
            detail: Some(detail.into()),
            details: None,
            retry_after: Some(retry_after),
        }
    }

    pub fn unprocessable_entity(error: impl Into<String>, details: serde_json::Value) -> Self {
        Self {
            error: error.into(),
            detail: Some("request body violates supplemental write contract".into()),
            details: Some(details),
            retry_after: None,
        }
    }

    pub fn conflict(error: impl Into<String>, details: serde_json::Value) -> Self {
        Self {
            error: error.into(),
            detail: Some("supplemental write conflicts with existing state".into()),
            details: Some(details),
            retry_after: None,
        }
    }
}

/// HTTP headers for LETHE responses (M14 §4.1).
#[derive(Debug, Clone)]
pub struct LetheHeaders {
    pub projection_id: String,
    pub read_mode: String,
    pub stale: bool,
    pub built_at: String,
    #[allow(dead_code)]
    pub lineage_ref: Option<String>,
}

impl From<&ProjectionMetadata> for LetheHeaders {
    fn from(meta: &ProjectionMetadata) -> Self {
        Self {
            projection_id: meta.projection_id.as_str().to_string(),
            read_mode: serde_json::to_string(&meta.read_mode)
                .unwrap_or_else(|_| "unknown".into())
                .trim_matches('"')
                .to_string(),
            stale: meta.stale,
            built_at: meta.built_at.to_rfc3339(),
            lineage_ref: meta.lineage_ref.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> ProjectionMetadata {
        ProjectionMetadata {
            projection_id: ProjectionRef::new("proj:person-page"),
            version: SemVer::new("1.0.0"),
            built_at: Utc::now(),
            read_mode: ReadMode::OperationalLatest,
            stale: false,
            lineage_ref: Some("lineage:proj-person-page:build-42".into()),
        }
    }

    #[test]
    fn envelope_serializes_with_metadata() {
        let envelope = ResponseEnvelope {
            data: serde_json::json!({"persons": []}),
            projection_metadata: sample_meta(),
        };
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("projection_metadata"));
        assert!(json.contains("proj:person-page"));
    }

    #[test]
    fn error_response_variants() {
        let e = ErrorResponse::bad_request("invalid param");
        assert_eq!(e.error, "bad_request");

        let e = ErrorResponse::internal_server_error("boom");
        assert_eq!(e.error, "internal_server_error");

        let e = ErrorResponse::not_found();
        assert_eq!(e.error, "not_found");

        let e = ErrorResponse::service_unavailable(30);
        assert_eq!(e.retry_after, Some(30));
    }

    #[test]
    fn headers_from_metadata() {
        let meta = sample_meta();
        let headers = LetheHeaders::from(&meta);
        assert_eq!(headers.projection_id, "proj:person-page");
        assert!(!headers.stale);
        assert_eq!(headers.read_mode, "operational_latest");
    }

    #[test]
    fn stale_envelope() {
        let mut meta = sample_meta();
        meta.stale = true;
        let envelope = ResponseEnvelope {
            data: serde_json::json!(null),
            projection_metadata: meta,
        };
        assert!(envelope.projection_metadata.stale);
    }
}
