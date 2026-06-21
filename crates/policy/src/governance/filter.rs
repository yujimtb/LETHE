use sha2::Digest;

use crate::governance::types::{AccessScope, MaskStrategy, RestrictedFieldSpec};

// ---------------------------------------------------------------------------
// FilteringGate — filtering-before-exposure (L6)
//
// The final gate before data reaches external consumers.
// Enforces the Filtering-before-Exposure Law by masking or excluding
// restricted fields based on the viewer's access scope.
// ---------------------------------------------------------------------------

pub struct FilteringGate;

/// Outcome of filtering a single record (JSON object).
#[derive(Debug, Clone, PartialEq)]
pub struct FilterResult {
    /// The (possibly masked) JSON payload.
    pub payload: serde_json::Value,
    /// Fields that were masked/excluded.
    pub masked_fields: Vec<String>,
}

impl FilteringGate {
    /// Apply filtering-before-exposure to a JSON payload.
    ///
    /// Fields whose `RestrictedFieldSpec.level` exceeds the viewer's
    /// `AccessScope` are masked or removed according to their `MaskStrategy`.
    pub fn filter(
        payload: &serde_json::Value,
        viewer_scope: AccessScope,
        field_specs: &[RestrictedFieldSpec],
    ) -> FilterResult {
        let mut out = payload.clone();
        let mut masked = Vec::new();

        if !Self::retain_visible(&mut out) {
            out = serde_json::Value::Null;
            masked.push("visibility".to_string());
        }

        for spec in field_specs {
            if Self::should_mask(spec.level, viewer_scope)
                && Self::apply_mask(&mut out, &spec.field_path, spec.mask_strategy)
            {
                masked.push(spec.field_path.clone());
            }
        }

        FilterResult {
            payload: out,
            masked_fields: masked,
        }
    }

    fn retain_visible(value: &mut serde_json::Value) -> bool {
        match value {
            serde_json::Value::Object(obj) => {
                if obj
                    .get("visibility")
                    .or_else(|| obj.get("Visibility"))
                    .and_then(|raw| raw.as_bool())
                    == Some(false)
                {
                    return false;
                }
                for child in obj.values_mut() {
                    if !Self::retain_visible(child) {
                        *child = serde_json::Value::Null;
                    }
                }
                true
            }
            serde_json::Value::Array(items) => {
                let mut index = 0;
                while index < items.len() {
                    if Self::retain_visible(&mut items[index]) {
                        index += 1;
                    } else {
                        items.remove(index);
                    }
                }
                true
            }
            _ => true,
        }
    }

    /// Returns true if the field's access scope is higher than the viewer's.
    fn should_mask(field_level: AccessScope, viewer_scope: AccessScope) -> bool {
        Self::scope_rank(field_level) > Self::scope_rank(viewer_scope)
    }

    fn scope_rank(scope: AccessScope) -> u8 {
        match scope {
            AccessScope::Public => 0,
            AccessScope::Internal => 1,
            AccessScope::Restricted => 2,
            AccessScope::HighlySensitive => 3,
        }
    }

    /// Apply a masking strategy to a top-level field in a JSON object.
    /// Returns true if the field was found and masked.
    fn apply_mask(value: &mut serde_json::Value, field_path: &str, strategy: MaskStrategy) -> bool {
        match value {
            serde_json::Value::Object(obj) => {
                // Support single-level dotted paths (e.g., "contact.email")
                let parts: Vec<&str> = field_path.splitn(2, '.').collect();

                if parts.len() == 1 {
                    let key = parts[0];
                    if obj.contains_key(key) {
                        match strategy {
                            MaskStrategy::Exclude => {
                                obj.remove(key);
                            }
                            MaskStrategy::Redact => {
                                obj.insert(
                                    key.to_string(),
                                    serde_json::Value::String("[REDACTED]".into()),
                                );
                            }
                            MaskStrategy::Hash => {
                                if let Some(val) = obj.get(key) {
                                    let hash = format!(
                                        "{:x}",
                                        sha2::Digest::finalize(sha2::Sha256::new_with_prefix(
                                            val.to_string().as_bytes(),
                                        ),)
                                    );
                                    obj.insert(key.to_string(), serde_json::Value::String(hash));
                                }
                            }
                        }
                        true
                    } else {
                        let mut masked = false;
                        for child in obj.values_mut() {
                            masked |= Self::apply_mask(child, field_path, strategy);
                        }
                        masked
                    }
                } else {
                    let parent_key = parts[0];
                    let child_path = parts[1];
                    if let Some(child) = obj.get_mut(parent_key) {
                        Self::apply_mask(child, child_path, strategy)
                    } else {
                        let mut masked = false;
                        for child in obj.values_mut() {
                            masked |= Self::apply_mask(child, field_path, strategy);
                        }
                        masked
                    }
                }
            }
            serde_json::Value::Array(items) => {
                let mut masked = false;
                for item in items {
                    masked |= Self::apply_mask(item, field_path, strategy);
                }
                masked
            }
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn restricted_fields() -> Vec<RestrictedFieldSpec> {
        vec![
            RestrictedFieldSpec {
                field_path: "real_name".into(),
                level: AccessScope::Restricted,
                mask_strategy: MaskStrategy::Redact,
            },
            RestrictedFieldSpec {
                field_path: "email".into(),
                level: AccessScope::HighlySensitive,
                mask_strategy: MaskStrategy::Exclude,
            },
            RestrictedFieldSpec {
                field_path: "phone".into(),
                level: AccessScope::Restricted,
                mask_strategy: MaskStrategy::Hash,
            },
        ]
    }

    #[test]
    fn internal_viewer_sees_public_but_not_restricted() {
        let payload = json!({
            "display_name": "Alice",
            "real_name": "Alice Smith",
            "email": "alice@example.com",
            "phone": "090-1234-5678"
        });
        let result = FilteringGate::filter(&payload, AccessScope::Internal, &restricted_fields());
        // real_name: Restricted > Internal → redacted
        assert_eq!(result.payload["real_name"], "[REDACTED]");
        // email: HighlySensitive > Internal → excluded
        assert!(result.payload.get("email").is_none());
        // phone: Restricted > Internal → hashed
        assert_ne!(result.payload["phone"], "090-1234-5678");
        // display_name: unrestricted → untouched
        assert_eq!(result.payload["display_name"], "Alice");
        assert_eq!(result.masked_fields.len(), 3);
    }

    #[test]
    fn restricted_viewer_sees_restricted_but_not_highly_sensitive() {
        let payload = json!({
            "real_name": "Bob",
            "email": "bob@example.com"
        });
        let result = FilteringGate::filter(&payload, AccessScope::Restricted, &restricted_fields());
        // real_name: Restricted == Restricted → NOT masked
        assert_eq!(result.payload["real_name"], "Bob");
        // email: HighlySensitive > Restricted → excluded
        assert!(result.payload.get("email").is_none());
        assert_eq!(result.masked_fields.len(), 1);
    }

    #[test]
    fn public_viewer_everything_masked() {
        let payload = json!({
            "display_name": "Charlie",
            "real_name": "Charlie Brown",
            "email": "c@example.com",
            "phone": "080-0000-0000"
        });
        let result = FilteringGate::filter(&payload, AccessScope::Public, &restricted_fields());
        assert_eq!(result.payload["real_name"], "[REDACTED]");
        assert!(result.payload.get("email").is_none());
        assert_eq!(result.masked_fields.len(), 3);
    }

    #[test]
    fn highly_sensitive_viewer_sees_everything() {
        let payload = json!({
            "real_name": "Admin",
            "email": "admin@example.com",
            "phone": "070-0000-0000"
        });
        let result =
            FilteringGate::filter(&payload, AccessScope::HighlySensitive, &restricted_fields());
        assert_eq!(result.payload["real_name"], "Admin");
        assert_eq!(result.payload["email"], "admin@example.com");
        assert!(result.masked_fields.is_empty());
    }

    #[test]
    fn nested_field_path() {
        let payload = json!({
            "contact": { "email": "x@y.com", "phone": "000" }
        });
        let specs = vec![RestrictedFieldSpec {
            field_path: "contact.email".into(),
            level: AccessScope::HighlySensitive,
            mask_strategy: MaskStrategy::Exclude,
        }];
        let result = FilteringGate::filter(&payload, AccessScope::Internal, &specs);
        assert!(result.payload["contact"].get("email").is_none());
        assert_eq!(result.payload["contact"]["phone"], "000");
    }

    #[test]
    fn missing_field_is_noop() {
        let payload = json!({"name": "test"});
        let specs = vec![RestrictedFieldSpec {
            field_path: "nonexistent".into(),
            level: AccessScope::Restricted,
            mask_strategy: MaskStrategy::Exclude,
        }];
        let result = FilteringGate::filter(&payload, AccessScope::Public, &specs);
        assert!(result.masked_fields.is_empty());
        assert_eq!(result.payload, payload);
    }

    #[test]
    fn empty_specs_passes_through() {
        let payload = json!({"secret": "data"});
        let result = FilteringGate::filter(&payload, AccessScope::Public, &[]);
        assert_eq!(result.payload, payload);
        assert!(result.masked_fields.is_empty());
    }

    #[test]
    fn array_root_is_masked() {
        let payload = json!([
            {"email": "alice@example.com"},
            {"email": "bob@example.com"}
        ]);
        let specs = vec![RestrictedFieldSpec {
            field_path: "email".into(),
            level: AccessScope::HighlySensitive,
            mask_strategy: MaskStrategy::Exclude,
        }];
        let result = FilteringGate::filter(&payload, AccessScope::Internal, &specs);
        assert!(result.payload[0].get("email").is_none());
        assert!(result.payload[1].get("email").is_none());
    }

    #[test]
    fn descendant_field_inside_paginated_array_is_masked() {
        let payload = json!({
            "data": [
                {"identities": ["slack:U1"], "display_name": "Alice"},
                {"identities": ["slack:U2"], "display_name": "Bob"}
            ],
            "total": 2
        });
        let specs = vec![RestrictedFieldSpec {
            field_path: "identities".into(),
            level: AccessScope::Restricted,
            mask_strategy: MaskStrategy::Exclude,
        }];
        let result = FilteringGate::filter(&payload, AccessScope::Internal, &specs);
        assert!(result.payload["data"][0].get("identities").is_none());
        assert!(result.payload["data"][1].get("identities").is_none());
    }

    #[test]
    fn visibility_false_objects_are_removed_before_exposure() {
        let payload = json!({
            "data": [
                {"person_id": "visible", "Visibility": true, "display_name": "Visible"},
                {"person_id": "hidden", "Visibility": false, "display_name": "Hidden"}
            ],
            "total": 2
        });

        let result = FilteringGate::filter(&payload, AccessScope::Internal, &[]);

        assert_eq!(result.payload["data"].as_array().unwrap().len(), 1);
        assert_eq!(result.payload["data"][0]["person_id"], "visible");
        assert!(!result.payload.to_string().contains("hidden"));
    }
}
