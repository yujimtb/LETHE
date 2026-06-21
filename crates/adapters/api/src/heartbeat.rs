//! M09 — Heartbeat Observation generator
//!
//! Heartbeat is a periodic "I am alive" signal emitted by every adapter.
//! Monitoring service detects heartbeat absence and alerts.

use chrono::{DateTime, Utc};

use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};

use super::idempotency::heartbeat_key;
use super::traits::ObservationDraft;

/// Schema identifier for heartbeat observations.
pub const HEARTBEAT_SCHEMA: &str = "schema:observer-heartbeat";
pub const HEARTBEAT_SCHEMA_VERSION: &str = "1.0.0";

/// Generate a heartbeat `ObservationDraft`.
///
/// The `window_start` is a truncated ISO-8601 timestamp (e.g.
/// minute-level) that makes the idempotency key unique per heartbeat
/// interval while remaining deterministic.
pub fn heartbeat_draft(
    observer: &ObserverRef,
    source_system: &SourceSystemRef,
    now: DateTime<Utc>,
    pending_count: u64,
    last_successful_capture: Option<DateTime<Utc>>,
) -> ObservationDraft {
    let window = now.format("%Y-%m-%dT%H:%M").to_string();
    let observer_name = observer
        .as_str()
        .strip_prefix("obs:")
        .unwrap_or(observer.as_str());

    ObservationDraft {
        schema: SchemaRef::new(HEARTBEAT_SCHEMA),
        schema_version: SemVer::new(HEARTBEAT_SCHEMA_VERSION),
        observer: observer.clone(),
        source_system: Some(source_system.clone()),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("observer:{observer_name}")),
        target: None,
        payload: serde_json::json!({
            "status": "alive",
            "last_successful_capture_at": last_successful_capture,
            "pending_count": pending_count,
        }),
        attachments: vec![],
        published: now,
        idempotency_key: heartbeat_key(observer_name, &window),
        meta: serde_json::json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_is_deterministic_within_same_minute() {
        let obs = ObserverRef::new("obs:slack-crawler");
        let sys = SourceSystemRef::new("sys:slack");
        let t1 = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:30:00Z")
            .unwrap()
            .to_utc();
        let t2 = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:30:45Z")
            .unwrap()
            .to_utc();

        let d1 = heartbeat_draft(&obs, &sys, t1, 0, None);
        let d2 = heartbeat_draft(&obs, &sys, t2, 0, None);
        assert_eq!(d1.idempotency_key, d2.idempotency_key);
    }

    #[test]
    fn heartbeat_differs_across_minutes() {
        let obs = ObserverRef::new("obs:slack-crawler");
        let sys = SourceSystemRef::new("sys:slack");
        let t1 = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:30:00Z")
            .unwrap()
            .to_utc();
        let t2 = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:31:00Z")
            .unwrap()
            .to_utc();

        let d1 = heartbeat_draft(&obs, &sys, t1, 0, None);
        let d2 = heartbeat_draft(&obs, &sys, t2, 0, None);
        assert_ne!(d1.idempotency_key, d2.idempotency_key);
    }

    #[test]
    fn heartbeat_payload_contains_status() {
        let obs = ObserverRef::new("obs:test");
        let sys = SourceSystemRef::new("sys:test");
        let now = Utc::now();
        let draft = heartbeat_draft(&obs, &sys, now, 3, Some(now));
        assert_eq!(draft.payload["status"], "alive");
        assert_eq!(draft.payload["pending_count"], 3);
    }
}
