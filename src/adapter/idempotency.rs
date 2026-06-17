//! M09 — Deterministic idempotency key generation
//!
//! Same source data → same key. Keys are isolated per adapter.

use crate::domain::IdempotencyKey;
use sha2::Digest;
use unicode_normalization::UnicodeNormalization;

pub const CANONICAL_JSON_META_KEY: &str = "canonical_json";
pub const OBJECT_ID_META_KEY: &str = "object_id";

pub fn identity_key(source: &str, object_id: &str, canonical_json: &str) -> IdempotencyKey {
    let hash = hex::encode(sha2::Sha256::digest(canonical_json.as_bytes()));
    IdempotencyKey::new(format!("{source}:{object_id}:{hash}"))
}

pub fn canonical_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).expect("canonical tuple must serialize")
}

pub fn normalize_canonical_body(body: &str) -> String {
    body.replace("\r\n", "\n").nfc().collect()
}

/// Slack idempotency key patterns (M10).
pub fn slack_message_key(channel_id: &str, ts: &str) -> IdempotencyKey {
    IdempotencyKey::new(format!("slack:{channel_id}:{ts}"))
}

pub fn slack_edit_key(channel_id: &str, ts: &str, edit_ts: &str) -> IdempotencyKey {
    IdempotencyKey::new(format!("slack:{channel_id}:{ts}:edit:{edit_ts}"))
}

pub fn slack_delete_key(channel_id: &str, ts: &str) -> IdempotencyKey {
    IdempotencyKey::new(format!("slack:{channel_id}:{ts}:delete"))
}

pub fn slack_reaction_key(
    channel_id: &str,
    ts: &str,
    user: &str,
    emoji: &str,
) -> IdempotencyKey {
    IdempotencyKey::new(format!("slack:{channel_id}:{ts}:react:{user}:{emoji}"))
}

pub fn slack_file_key(channel_id: &str, ts: &str, file_id: &str) -> IdempotencyKey {
    IdempotencyKey::new(format!("slack:{channel_id}:{ts}:file:{file_id}"))
}

/// Google Slides idempotency key pattern (M11).
pub fn gslides_revision_key(presentation_id: &str, revision_id: &str) -> IdempotencyKey {
    IdempotencyKey::new(format!(
        "gslides:{presentation_id}:rev:{revision_id}"
    ))
}

/// Heartbeat idempotency key — includes observer name and timestamp window
/// to allow one heartbeat per interval.
pub fn heartbeat_key(observer_name: &str, window_start: &str) -> IdempotencyKey {
    IdempotencyKey::new(format!("heartbeat:{observer_name}:{window_start}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slack_message_key_deterministic() {
        let k1 = slack_message_key("C01ABC", "1234567890.123456");
        let k2 = slack_message_key("C01ABC", "1234567890.123456");
        assert_eq!(k1, k2);
        assert_eq!(k1.as_str(), "slack:C01ABC:1234567890.123456");
    }

    #[test]
    fn slack_edit_key_includes_edit_ts() {
        let k = slack_edit_key("C01ABC", "1234567890.123456", "1234567891.000000");
        assert_eq!(
            k.as_str(),
            "slack:C01ABC:1234567890.123456:edit:1234567891.000000"
        );
    }

    #[test]
    fn slack_delete_key_format() {
        let k = slack_delete_key("C01ABC", "1234567890.123456");
        assert_eq!(k.as_str(), "slack:C01ABC:1234567890.123456:delete");
    }

    #[test]
    fn slack_reaction_key_format() {
        let k = slack_reaction_key("C01ABC", "1234567890.123456", "U01XYZ", "thumbsup");
        assert_eq!(
            k.as_str(),
            "slack:C01ABC:1234567890.123456:react:U01XYZ:thumbsup"
        );
    }

    #[test]
    fn slack_file_key_format() {
        let k = slack_file_key("C01ABC", "1234567890.123456", "F01DEF");
        assert_eq!(
            k.as_str(),
            "slack:C01ABC:1234567890.123456:file:F01DEF"
        );
    }

    #[test]
    fn gslides_revision_key_deterministic() {
        let k1 = gslides_revision_key("pres123", "rev456");
        let k2 = gslides_revision_key("pres123", "rev456");
        assert_eq!(k1, k2);
        assert_eq!(k1.as_str(), "gslides:pres123:rev:rev456");
    }

    #[test]
    fn different_sources_never_collide() {
        let slack = slack_message_key("C01", "123");
        let gslides = gslides_revision_key("C01", "123");
        assert_ne!(slack, gslides);
    }

    #[test]
    fn identity_key_hashes_canonical_json() {
        let canonical = canonical_json(&serde_json::json!({
            "body": "hello",
            "event_time": "2026-05-01T00:00:00Z",
        }));
        let key = identity_key("slack", "channel:C01:ts:1", &canonical);

        assert!(key.as_str().starts_with("slack:channel:C01:ts:1:"));
        assert_eq!(key.as_str().rsplit(':').next().unwrap().len(), 64);
    }

    #[test]
    fn canonical_body_normalizes_transport_noise_only() {
        assert_eq!(normalize_canonical_body("a\r\nb"), "a\nb");
        assert_eq!(normalize_canonical_body("a  b"), "a  b");
    }
}
