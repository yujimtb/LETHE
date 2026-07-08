//! M02 Registry — Supplemental kind schema definitions and validation.

use chrono::{DateTime, Utc};
use jsonschema::error::ValidationErrorKind;
use serde::{Deserialize, Serialize};

use lethe_core::domain::{SemVer, SupplementalId, SupplementalRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupplementalKindSchema {
    /// Unversioned kind name, e.g. `claim`.
    pub kind: String,
    pub version: SemVer,
    #[serde(default = "default_anchor_required")]
    pub anchor_required: bool,
    /// JSON Schema document for the supplemental payload.
    pub payload_schema: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupplementalKindVersion {
    pub kind: String,
    pub version: SemVer,
    pub anchor_required: bool,
    pub payload_schema: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupplementalKindValidationConfig {
    pub reject_unregistered_kinds: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldViolation {
    pub field: String,
    pub keyword: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupplementalKindError {
    KindNotRegistered {
        kind: String,
        major_version: u64,
    },
    UnregisteredKindPolicyDisabled {
        kind: String,
        major_version: u64,
    },
    InvalidKindRef {
        kind_ref: String,
        message: String,
    },
    InvalidJsonSchema {
        kind: String,
        version: SemVer,
        message: String,
    },
    PayloadSchemaViolation {
        kind: String,
        major_version: u64,
        violations: Vec<FieldViolation>,
    },
    MissingRequiredAnchor {
        kind_ref: String,
    },
    MissingOriginMetadata {
        kind_ref: String,
        violations: Vec<FieldViolation>,
    },
    SchemaVersionRuleViolation {
        kind: String,
        current_version: SemVer,
        next_version: SemVer,
        message: String,
    },
    MissingClaimSupplementalAnchor {
        kind_ref: String,
        referenced_supplementals: Vec<String>,
    },
}

impl std::fmt::Display for SupplementalKindError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KindNotRegistered {
                kind,
                major_version,
            } => write!(
                formatter,
                "supplemental kind {kind}@{major_version} is not registered"
            ),
            Self::UnregisteredKindPolicyDisabled {
                kind,
                major_version,
            } => write!(
                formatter,
                "supplemental.reject_unregistered_kinds is false while resolving unregistered kind {kind}@{major_version}"
            ),
            Self::InvalidKindRef { kind_ref, message } => {
                write!(
                    formatter,
                    "invalid supplemental kind ref {kind_ref}: {message}"
                )
            }
            Self::InvalidJsonSchema {
                kind,
                version,
                message,
            } => write!(
                formatter,
                "supplemental kind schema {kind}@{} is not valid JSON Schema: {message}",
                semver_major_display(version)
            ),
            Self::PayloadSchemaViolation {
                kind,
                major_version,
                violations,
            } => write!(
                formatter,
                "supplemental payload for {kind}@{major_version} violates schema: {} violation(s)",
                violations.len()
            ),
            Self::MissingRequiredAnchor { kind_ref } => write!(
                formatter,
                "{kind_ref} requires at least one derived_from anchor"
            ),
            Self::MissingOriginMetadata {
                kind_ref,
                violations,
            } => write!(
                formatter,
                "{kind_ref} requires origin metadata: {} violation(s)",
                violations.len()
            ),
            Self::SchemaVersionRuleViolation {
                kind,
                current_version,
                next_version,
                message,
            } => write!(
                formatter,
                "supplemental kind {kind} version transition {current_version} -> {next_version} violates schema version rules: {message}"
            ),
            Self::MissingClaimSupplementalAnchor {
                kind_ref,
                referenced_supplementals,
            } => write!(
                formatter,
                "{kind_ref} must derive from at least one claim supplemental; referenced supplementals: {:?}",
                referenced_supplementals
            ),
        }
    }
}

impl std::error::Error for SupplementalKindError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupplementalKindRef {
    pub kind: String,
    pub major_version: u64,
}

pub fn parse_supplemental_kind_ref(
    kind_ref: &str,
) -> Result<SupplementalKindRef, SupplementalKindError> {
    let Some((kind, major)) = kind_ref.split_once('@') else {
        return Err(SupplementalKindError::InvalidKindRef {
            kind_ref: kind_ref.to_owned(),
            message: "expected format kind@major".to_owned(),
        });
    };
    if kind.trim().is_empty() {
        return Err(SupplementalKindError::InvalidKindRef {
            kind_ref: kind_ref.to_owned(),
            message: "kind must not be blank".to_owned(),
        });
    }
    if major.contains('@') {
        return Err(SupplementalKindError::InvalidKindRef {
            kind_ref: kind_ref.to_owned(),
            message: "expected exactly one @ separator".to_owned(),
        });
    }
    let major_version =
        major
            .parse::<u64>()
            .map_err(|_| SupplementalKindError::InvalidKindRef {
                kind_ref: kind_ref.to_owned(),
                message: "major version must be an integer".to_owned(),
            })?;
    Ok(SupplementalKindRef {
        kind: kind.to_owned(),
        major_version,
    })
}

pub fn supplemental_kind_key(kind: &str, major_version: u64) -> String {
    format!("{kind}@{major_version}")
}

pub fn supplemental_kind_key_for_schema(
    schema: &SupplementalKindSchema,
) -> Result<String, SupplementalKindError> {
    Ok(supplemental_kind_key(
        &schema.kind,
        semver_major(&schema.version).map_err(|message| {
            SupplementalKindError::InvalidJsonSchema {
                kind: schema.kind.clone(),
                version: schema.version.clone(),
                message,
            }
        })?,
    ))
}

pub fn validate_supplemental_payload(
    schema: &SupplementalKindSchema,
    payload: &serde_json::Value,
) -> Result<(), SupplementalKindError> {
    let major_version = semver_major(&schema.version).map_err(|message| {
        SupplementalKindError::InvalidJsonSchema {
            kind: schema.kind.clone(),
            version: schema.version.clone(),
            message,
        }
    })?;
    validate_json_schema_document(schema)?;
    let validator = jsonschema::validator_for(&schema.payload_schema).map_err(|error| {
        SupplementalKindError::InvalidJsonSchema {
            kind: schema.kind.clone(),
            version: schema.version.clone(),
            message: error.to_string(),
        }
    })?;
    let violations = validator
        .iter_errors(payload)
        .flat_map(field_violations_from_error)
        .collect::<Vec<_>>();
    if violations.is_empty() {
        Ok(())
    } else {
        Err(SupplementalKindError::PayloadSchemaViolation {
            kind: schema.kind.clone(),
            major_version,
            violations,
        })
    }
}

pub fn validate_supplemental_anchor_policy(
    schema: &SupplementalKindSchema,
    record: &SupplementalRecord,
) -> Result<(), SupplementalKindError> {
    let kind_ref = supplemental_kind_key_for_schema(schema)?;
    let empty_anchor = record.derived_from.observations.is_empty()
        && record.derived_from.blobs.is_empty()
        && record.derived_from.supplementals.is_empty();

    if schema.anchor_required && empty_anchor {
        return Err(SupplementalKindError::MissingRequiredAnchor { kind_ref });
    }

    if !schema.anchor_required {
        validate_origin_metadata(&kind_ref, &record.payload)?;
    }

    Ok(())
}

pub fn validate_supplemental_record_claim_anchor<F>(
    record: &SupplementalRecord,
    mut supplemental_kind_for_id: F,
) -> Result<(), SupplementalKindError>
where
    F: FnMut(&SupplementalId) -> Option<String>,
{
    if !requires_claim_supplemental_anchor(&record.kind) {
        return Ok(());
    }

    let referenced_supplementals = record
        .derived_from
        .supplementals
        .iter()
        .map(|id| id.as_str().to_owned())
        .collect::<Vec<_>>();
    let has_claim_anchor = record
        .derived_from
        .supplementals
        .iter()
        .any(|id| supplemental_kind_for_id(id).is_some_and(|kind_ref| kind_ref == "claim@1"));

    if has_claim_anchor {
        Ok(())
    } else {
        Err(SupplementalKindError::MissingClaimSupplementalAnchor {
            kind_ref: record.kind.clone(),
            referenced_supplementals,
        })
    }
}

pub fn validate_json_schema_document(
    schema: &SupplementalKindSchema,
) -> Result<(), SupplementalKindError> {
    jsonschema::meta::validate(&schema.payload_schema).map_err(|error| {
        SupplementalKindError::InvalidJsonSchema {
            kind: schema.kind.clone(),
            version: schema.version.clone(),
            message: error.to_string(),
        }
    })
}

pub fn base_supplemental_kind_schemas() -> Vec<SupplementalKindSchema> {
    vec![
        SupplementalKindSchema {
            kind: "claim".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["statement", "verification_mode"],
                "properties": {
                    "statement": { "type": "string" },
                    "verification_mode": {
                        "type": "string",
                        "enum": ["check", "generate"]
                    },
                    "context": { "type": "string" },
                    "source_quote": { "type": "string" },
                    "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                    "backfill": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "decision".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["statement"],
                "properties": {
                    "statement": { "type": "string" },
                    "rationale": { "type": "string" },
                    "alternatives": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "supersedes": { "type": "string" },
                    "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                    "backfill": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "parking".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["statement", "resume_context"],
                "properties": {
                    "statement": { "type": "string" },
                    "resume_context": { "type": "string" },
                    "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                    "backfill": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "verification-result".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["verdict", "reasoning"],
                "properties": {
                    "verdict": {
                        "type": "string",
                        "enum": ["consistent", "inconsistent", "inconclusive"]
                    },
                    "reasoning": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "claim-transition".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["to_state"],
                "properties": {
                    "to_state": {
                        "type": "string",
                        "enum": [
                            "open",
                            "dispatched",
                            "verified",
                            "refuted",
                            "inconclusive",
                            "terminated",
                            "parked"
                        ]
                    },
                    "reason": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "session-summary".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["summary"],
                "properties": {
                    "summary": { "type": "string" },
                    "topics": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                    "backfill": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "reply-draft".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["channel", "recipient", "body", "drafted_at"],
                "properties": {
                    "channel": { "type": "string", "enum": ["slack", "gmail", "discord", "tailscale-web"] },
                    "recipient": { "type": "string", "minLength": 1 },
                    "body": { "type": "string", "minLength": 1 },
                    "drafted_at": { "type": "string", "format": "date-time" },
                    "thread_ref": { "type": "string" },
                    "subject": { "type": "string" },
                    "expires_at": { "type": "string", "format": "date-time" },
                    "project": { "type": "string" },
                    "backfill": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "reply-approval".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["interface", "decision", "decided_at", "actor"],
                "properties": {
                    "interface": { "type": "string", "enum": ["slack", "discord", "tailscale-web"] },
                    "decision": { "type": "string", "enum": ["approved", "skipped"] },
                    "decided_at": { "type": "string", "format": "date-time" },
                    "actor": { "type": "string", "minLength": 1 },
                    "reason": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "send-record".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: true,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["channel", "sent_at", "mode"],
                "properties": {
                    "channel": { "type": "string", "enum": ["slack", "gmail", "discord"] },
                    "sent_at": { "type": "string", "format": "date-time" },
                    "mode": { "type": "string", "enum": ["approved", "automatic"] },
                    "approval_id": { "type": "string" },
                    "message_ref": { "type": "string" },
                    "auto_review": {
                        "type": "object",
                        "required": ["urgency", "recipient_safety", "content_safety"],
                        "properties": {
                            "urgency": { "type": "boolean" },
                            "recipient_safety": { "type": "boolean" },
                            "content_safety": { "type": "boolean" }
                        },
                        "additionalProperties": false
                    }
                },
                "allOf": [
                    {
                        "if": { "properties": { "mode": { "const": "automatic" } }, "required": ["mode"] },
                        "then": { "required": ["auto_review"] }
                    }
                ],
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "nudge-event".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: false,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["origin", "nudge_id", "occurred_at", "strategy", "state"],
                "properties": {
                    "origin": origin_schema(),
                    "nudge_id": { "type": "string", "minLength": 1 },
                    "occurred_at": { "type": "string", "format": "date-time" },
                    "strategy": { "type": "string", "minLength": 1 },
                    "state": { "type": "string", "enum": ["scheduled", "fired", "acknowledged", "missed"] }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "eos-state-transition".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: false,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["origin", "from_state", "to_state", "occurred_at"],
                "properties": {
                    "origin": origin_schema(),
                    "from_state": { "type": "string", "enum": ["pre_sleep", "sleeping", "awake", "in_bed_scrolling", "out_of_home"] },
                    "to_state": { "type": "string", "enum": ["pre_sleep", "sleeping", "awake", "in_bed_scrolling", "out_of_home"] },
                    "occurred_at": { "type": "string", "format": "date-time" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "mode-transition".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: false,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["origin", "from_mode", "to_mode", "occurred_at"],
                "properties": {
                    "origin": origin_schema(),
                    "from_mode": { "type": "string", "enum": ["focus", "triage", "away"] },
                    "to_mode": { "type": "string", "enum": ["focus", "triage", "away"] },
                    "occurred_at": { "type": "string", "format": "date-time" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "briefing-issue".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: false,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["origin", "briefing_id", "issued_at", "surface"],
                "properties": {
                    "origin": origin_schema(),
                    "briefing_id": { "type": "string", "minLength": 1 },
                    "issued_at": { "type": "string", "format": "date-time" },
                    "surface": { "type": "string", "enum": ["morning", "slack_dm", "audio"] },
                    "project": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "briefing-feedback".into(),
            version: SemVer::new("1.0.0"),
            anchor_required: false,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": [
                    "origin",
                    "feedback_id",
                    "rating",
                    "note",
                    "briefing_date",
                    "briefing_id",
                    "submitted_at",
                    "surface",
                    "project"
                ],
                "properties": {
                    "origin": origin_schema(),
                    "feedback_id": { "type": "string" },
                    "rating": { "type": "string", "enum": ["good", "bad"] },
                    "note": { "type": "string" },
                    "briefing_date": { "type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$" },
                    "briefing_id": { "type": "string" },
                    "submitted_at": { "type": "string", "format": "date-time" },
                    "surface": { "type": "string", "enum": ["cli", "serve-web"] },
                    "project": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
    ]
}

pub(crate) fn semver_major(version: &SemVer) -> Result<u64, String> {
    parse_semver(version.as_str())
        .map(|version| version.major)
        .ok_or_else(|| format!("version {version} is not SemVer"))
}

fn semver_major_display(version: &SemVer) -> String {
    semver_major(version)
        .map(|major| major.to_string())
        .unwrap_or_else(|_| version.as_str().to_owned())
}

fn requires_claim_supplemental_anchor(kind_ref: &str) -> bool {
    matches!(kind_ref, "verification-result@1" | "claim-transition@1")
}

fn default_anchor_required() -> bool {
    true
}

fn origin_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["actor", "occurred_at", "context_id"],
        "properties": {
            "actor": { "type": "string", "minLength": 1 },
            "occurred_at": { "type": "string", "format": "date-time" },
            "context_id": { "type": "string", "minLength": 1 }
        },
        "additionalProperties": false
    })
}

fn validate_origin_metadata(
    kind_ref: &str,
    payload: &serde_json::Value,
) -> Result<(), SupplementalKindError> {
    let origin_schema = SupplementalKindSchema {
        kind: "origin".to_owned(),
        version: SemVer::new("1.0.0"),
        anchor_required: true,
        payload_schema: origin_schema(),
        registered_by: None,
        registered_at: None,
    };
    let origin = payload.get("origin").unwrap_or(&serde_json::Value::Null);
    match validate_supplemental_payload(&origin_schema, origin) {
        Ok(()) => Ok(()),
        Err(SupplementalKindError::PayloadSchemaViolation { violations, .. }) => {
            Err(SupplementalKindError::MissingOriginMetadata {
                kind_ref: kind_ref.to_owned(),
                violations,
            })
        }
        Err(error) => Err(error),
    }
}

fn field_violations_from_error(error: jsonschema::ValidationError<'_>) -> Vec<FieldViolation> {
    let keyword = error
        .schema_path
        .as_str()
        .rsplit('/')
        .next()
        .filter(|keyword| !keyword.is_empty())
        .unwrap_or("schema")
        .to_owned();
    let message = error.masked().to_string();
    let fields = match &error.kind {
        ValidationErrorKind::Required { property } => property
            .as_str()
            .map(|property| vec![property.to_owned()])
            .unwrap_or_else(|| vec![field_from_instance_path(error.instance_path.as_str())]),
        ValidationErrorKind::AdditionalProperties { unexpected }
        | ValidationErrorKind::UnevaluatedProperties { unexpected } => unexpected.clone(),
        _ => vec![field_from_instance_path(error.instance_path.as_str())],
    };

    fields
        .into_iter()
        .map(|field| FieldViolation {
            field,
            keyword: keyword.clone(),
            message: message.clone(),
        })
        .collect()
}

fn field_from_instance_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "$".to_owned()
    } else {
        trimmed
            .replace('/', ".")
            .replace("~1", "/")
            .replace("~0", "~")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ParsedSemVer {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

pub(crate) fn parse_semver(raw: &str) -> Option<ParsedSemVer> {
    let mut parts = raw.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(ParsedSemVer {
        major,
        minor,
        patch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use lethe_core::domain::supplemental::InputAnchorSet;
    use lethe_core::domain::{ActorRef, Mutability};

    fn briefing_feedback_schema() -> SupplementalKindSchema {
        base_supplemental_kind_schemas()
            .into_iter()
            .find(|schema| schema.kind == "briefing-feedback")
            .unwrap()
    }

    fn briefing_feedback_payload(rating: &str, surface: &str) -> serde_json::Value {
        serde_json::json!({
            "origin": {
                "actor": "eos",
                "occurred_at": "2026-07-09T00:00:00Z",
                "context_id": "briefing-feedback"
            },
            "feedback_id": "feedback-1",
            "rating": rating,
            "note": "",
            "briefing_date": "2026-07-09",
            "briefing_id": "briefing-2026-07-09",
            "submitted_at": "2026-07-09T00:00:00Z",
            "surface": surface,
            "project": "eos"
        })
    }

    #[test]
    fn payload_violation_fields_include_required_type_and_enum() {
        let schema = base_supplemental_kind_schemas()
            .into_iter()
            .find(|schema| schema.kind == "claim")
            .unwrap();
        let err = validate_supplemental_payload(
            &schema,
            &serde_json::json!({
                "statement": 10,
                "verification_mode": "other"
            }),
        )
        .unwrap_err();

        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
            panic!("expected payload violation");
        };
        assert!(violations.iter().any(|v| v.field == "statement"));
        assert!(violations.iter().any(|v| v.field == "verification_mode"));
    }

    #[test]
    fn payload_violation_fields_include_missing_required_field() {
        let schema = base_supplemental_kind_schemas()
            .into_iter()
            .find(|schema| schema.kind == "claim")
            .unwrap();
        let err = validate_supplemental_payload(
            &schema,
            &serde_json::json!({
                "statement": "検証対象"
            }),
        )
        .unwrap_err();

        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
            panic!("expected payload violation");
        };
        assert!(violations.iter().any(|v| v.field == "verification_mode"));
    }

    #[test]
    fn base_supplemental_kind_schemas_include_briefing_feedback() {
        let schema = briefing_feedback_schema();
        assert_eq!(schema.kind, "briefing-feedback");
        assert_eq!(schema.version, SemVer::new("1.0.0"));
        assert!(!schema.anchor_required);
    }

    #[test]
    fn briefing_feedback_allows_empty_anchor_with_origin() {
        let schema = briefing_feedback_schema();
        let record = SupplementalRecord {
            id: SupplementalId::new("sup:briefing-feedback"),
            kind: "briefing-feedback@1".into(),
            derived_from: InputAnchorSet::default(),
            payload: briefing_feedback_payload("good", "cli"),
            created_by: ActorRef::new("actor:test"),
            created_at: Utc::now(),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        };

        validate_supplemental_payload(&schema, &record.payload).unwrap();
        validate_supplemental_anchor_policy(&schema, &record).unwrap();

        let missing_origin = SupplementalRecord {
            payload: serde_json::json!({
                "feedback_id": "feedback-1",
                "rating": "good",
                "note": "",
                "briefing_date": "2026-07-09",
                "briefing_id": "briefing-2026-07-09",
                "submitted_at": "2026-07-09T00:00:00Z",
                "surface": "cli",
                "project": "eos"
            }),
            ..record
        };
        let payload_err =
            validate_supplemental_payload(&schema, &missing_origin.payload).unwrap_err();
        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = payload_err else {
            panic!("expected payload violation");
        };
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "origin")
        );

        let anchor_err = validate_supplemental_anchor_policy(&schema, &missing_origin).unwrap_err();
        assert!(matches!(
            anchor_err,
            SupplementalKindError::MissingOriginMetadata { .. }
        ));
    }

    #[test]
    fn briefing_feedback_rejects_rating_and_surface_enum_violations() {
        let schema = briefing_feedback_schema();

        let err = validate_supplemental_payload(&schema, &briefing_feedback_payload("ok", "cli"))
            .unwrap_err();
        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
            panic!("expected rating payload violation");
        };
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "rating")
        );

        let err = validate_supplemental_payload(&schema, &briefing_feedback_payload("good", "web"))
            .unwrap_err();
        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
            panic!("expected surface payload violation");
        };
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "surface")
        );
    }

    #[test]
    fn claim_transition_requires_claim_supplemental_anchor() {
        let claim_id = SupplementalId::new("sup:claim");
        let transition = SupplementalRecord {
            id: SupplementalId::new("sup:transition"),
            kind: "claim-transition@1".into(),
            derived_from: InputAnchorSet {
                observations: vec![],
                blobs: vec![],
                supplementals: vec![claim_id.clone()],
            },
            payload: serde_json::json!({ "to_state": "verified" }),
            created_by: ActorRef::new("actor:test"),
            created_at: Utc::now(),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        };

        validate_supplemental_record_claim_anchor(&transition, |id| {
            if id == &claim_id {
                Some("claim@1".to_owned())
            } else {
                None
            }
        })
        .unwrap();

        let err = validate_supplemental_record_claim_anchor(&transition, |_| None).unwrap_err();
        assert!(matches!(
            err,
            SupplementalKindError::MissingClaimSupplementalAnchor { .. }
        ));
    }

    #[test]
    fn system_event_kind_allows_empty_anchor_only_with_origin() {
        let schema = base_supplemental_kind_schemas()
            .into_iter()
            .find(|schema| schema.kind == "nudge-event")
            .unwrap();
        assert!(!schema.anchor_required);

        let record = SupplementalRecord {
            id: SupplementalId::new("sup:nudge"),
            kind: "nudge-event@1".into(),
            derived_from: InputAnchorSet::default(),
            payload: serde_json::json!({
                "origin": {
                    "actor": "eos-wakeup",
                    "occurred_at": "2026-07-06T00:00:00Z",
                    "context_id": "wake-window"
                },
                "nudge_id": "nudge-1",
                "occurred_at": "2026-07-06T00:00:00Z",
                "strategy": "light",
                "state": "fired"
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: Utc::now(),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        };
        validate_supplemental_payload(&schema, &record.payload).unwrap();
        validate_supplemental_anchor_policy(&schema, &record).unwrap();

        let missing_origin = SupplementalRecord {
            payload: serde_json::json!({
                "nudge_id": "nudge-1",
                "occurred_at": "2026-07-06T00:00:00Z",
                "strategy": "light",
                "state": "fired"
            }),
            ..record
        };
        let err = validate_supplemental_anchor_policy(&schema, &missing_origin).unwrap_err();
        assert!(matches!(
            err,
            SupplementalKindError::MissingOriginMetadata { .. }
        ));
    }

    #[test]
    fn new_reply_kind_rejects_missing_required_payload_fields() {
        let schema = base_supplemental_kind_schemas()
            .into_iter()
            .find(|schema| schema.kind == "reply-approval")
            .unwrap();
        let err = validate_supplemental_payload(
            &schema,
            &serde_json::json!({
                "interface": "email",
                "decision": "maybe"
            }),
        )
        .unwrap_err();
        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
            panic!("expected payload violation");
        };
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "actor")
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "interface")
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "decision")
        );
    }

    #[test]
    fn new_cognition_kinds_reject_missing_required_and_enum_violations() {
        let schemas = base_supplemental_kind_schemas();
        let cognition_kinds = [
            "reply-draft",
            "reply-approval",
            "send-record",
            "nudge-event",
            "eos-state-transition",
            "mode-transition",
            "briefing-issue",
            "briefing-feedback",
        ];
        for kind in cognition_kinds {
            let schema = schemas.iter().find(|schema| schema.kind == kind).unwrap();
            let err = validate_supplemental_payload(schema, &serde_json::json!({})).unwrap_err();
            let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
                panic!("expected payload violation for {kind}");
            };
            assert!(
                !violations.is_empty(),
                "expected required-field violations for {kind}"
            );
        }

        let origin = serde_json::json!({
            "actor": "system",
            "occurred_at": "2026-07-06T00:00:00Z",
            "context_id": "fixture"
        });
        let enum_cases = vec![
            (
                "reply-draft",
                "channel",
                serde_json::json!({
                    "channel": "email",
                    "recipient": "user",
                    "body": "hello",
                    "drafted_at": "2026-07-06T00:00:00Z"
                }),
            ),
            (
                "reply-approval",
                "decision",
                serde_json::json!({
                    "interface": "slack",
                    "decision": "maybe",
                    "decided_at": "2026-07-06T00:00:00Z",
                    "actor": "user"
                }),
            ),
            (
                "send-record",
                "mode",
                serde_json::json!({
                    "channel": "slack",
                    "sent_at": "2026-07-06T00:00:00Z",
                    "mode": "manual"
                }),
            ),
            (
                "nudge-event",
                "state",
                serde_json::json!({
                    "origin": origin.clone(),
                    "nudge_id": "nudge-1",
                    "occurred_at": "2026-07-06T00:00:00Z",
                    "strategy": "light",
                    "state": "unknown"
                }),
            ),
            (
                "eos-state-transition",
                "to_state",
                serde_json::json!({
                    "origin": origin.clone(),
                    "from_state": "sleeping",
                    "to_state": "dismissed",
                    "occurred_at": "2026-07-06T00:00:00Z"
                }),
            ),
            (
                "mode-transition",
                "to_mode",
                serde_json::json!({
                    "origin": origin.clone(),
                    "from_mode": "focus",
                    "to_mode": "sleep",
                    "occurred_at": "2026-07-06T00:00:00Z"
                }),
            ),
            (
                "briefing-issue",
                "surface",
                serde_json::json!({
                    "origin": origin,
                    "briefing_id": "briefing-1",
                    "issued_at": "2026-07-06T00:00:00Z",
                    "surface": "paper"
                }),
            ),
            (
                "briefing-feedback",
                "rating",
                serde_json::json!({
                    "origin": serde_json::json!({
                        "actor": "eos",
                        "occurred_at": "2026-07-06T00:00:00Z",
                        "context_id": "briefing-feedback"
                    }),
                    "feedback_id": "feedback-1",
                    "rating": "ok",
                    "note": "",
                    "briefing_date": "2026-07-06",
                    "briefing_id": "briefing-1",
                    "submitted_at": "2026-07-06T00:00:00Z",
                    "surface": "cli",
                    "project": "eos"
                }),
            ),
        ];
        for (kind, field, payload) in enum_cases {
            let schema = schemas.iter().find(|schema| schema.kind == kind).unwrap();
            let err = validate_supplemental_payload(schema, &payload).unwrap_err();
            let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
                panic!("expected enum violation for {kind}");
            };
            assert!(
                violations.iter().any(|violation| violation.field == field),
                "expected {kind} to reject enum field {field}, got {violations:?}"
            );
        }
    }
}
