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
                    "source_quote": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "decision".into(),
            version: SemVer::new("1.0.0"),
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
                    "supersedes": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "parking".into(),
            version: SemVer::new("1.0.0"),
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["statement", "resume_context"],
                "properties": {
                    "statement": { "type": "string" },
                    "resume_context": { "type": "string" }
                },
                "additionalProperties": false
            }),
            registered_by: Some("system:initial-supplemental-kind-registry".into()),
            registered_at: None,
        },
        SupplementalKindSchema {
            kind: "verification-result".into(),
            version: SemVer::new("1.0.0"),
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
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["summary"],
                "properties": {
                    "summary": { "type": "string" },
                    "topics": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
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
}
