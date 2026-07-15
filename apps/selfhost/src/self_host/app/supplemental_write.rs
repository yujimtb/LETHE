use std::collections::{HashMap, HashSet};

use chrono::Utc;
use lethe_core::domain::supplemental::{ConsentMetadata, InputAnchorSet};
use lethe_core::domain::{
    ActorRef, LineageRef, Mutability, ObservationId, SupplementalId, SupplementalRecord,
};
use lethe_registry::registry::{SupplementalKindError, SupplementalKindValidationConfig};

use super::*;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SupplementalWriteRequest {
    pub id: SupplementalId,
    pub kind: String,
    pub derived_from: InputAnchorSet,
    pub payload: serde_json::Value,
    pub created_by: ActorRef,
    pub mutability: Mutability,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub consent_metadata: Option<ConsentMetadata>,
    #[serde(default)]
    pub lineage: Option<LineageRef>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WriteEnvelope<T: serde::Serialize> {
    pub data: T,
}

impl AppService {
    pub fn write_supplemental(
        &self,
        request: SupplementalWriteRequest,
    ) -> Result<SupplementalRecord, SelfHostError> {
        let _bulk_import_operation = self.bulk_import_operation_lock()?;
        self.ensure_bulk_import_session_inactive("supplemental write")?;
        validate_supplemental_id(&request.id)?;

        let payload_bytes = serde_json::to_vec(&request.payload)?.len();
        if payload_bytes > self.config.resource_limits.max_payload_bytes {
            return Err(SelfHostError::SupplementalValidation {
                code: "payload_too_large",
                detail: serde_json::json!({
                    "field": "payload",
                    "actual_bytes": payload_bytes,
                    "max_bytes": self.config.resource_limits.max_payload_bytes
                }),
            });
        }

        let record = SupplementalRecord {
            id: request.id,
            kind: request.kind,
            derived_from: request.derived_from,
            payload: request.payload,
            created_by: request.created_by,
            created_at: Utc::now(),
            mutability: request.mutability,
            record_version: None,
            model_version: request.model_version,
            consent_metadata: request.consent_metadata,
            lineage: request.lineage,
        };

        let resolved_observation_ids =
            self.resolve_observation_anchors(&record.derived_from.observations)?;
        let resolved_supplemental_kinds =
            self.resolve_supplemental_anchors(&record.derived_from.supplementals)?;
        let resolved_supplemental_ids = resolved_supplemental_kinds
            .keys()
            .cloned()
            .collect::<HashSet<_>>();

        {
            let core = self.core_lock()?;
            core.registry
                .validate_supplemental_record_kind(
                    SupplementalKindValidationConfig {
                        reject_unregistered_kinds: self
                            .config
                            .supplemental
                            .reject_unregistered_kinds,
                    },
                    &record,
                    |id| resolved_supplemental_kinds.get(id).cloned(),
                )
                .map_err(map_supplemental_kind_error)?;
        }

        if self
            .persistence_lock()?
            .supplemental_by_id(&record.id)?
            .is_some()
        {
            return Err(append_only_conflict(&record.id));
        }

        let mut core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:person-page")?;
        if core.supplemental.get(&record.id).is_some() {
            return Err(append_only_conflict(&record.id));
        }

        let rollback = core
            .upsert_supplemental_checked(
                record,
                |observation_id| resolved_observation_ids.contains(observation_id),
                |supplemental_id| resolved_supplemental_ids.contains(supplemental_id),
            )
            .map_err(map_supplemental_store_error)?;
        let Some(persisted_record) = core.supplemental.get(&rollback.id).cloned() else {
            core.rollback_supplemental(rollback);
            return Err(SelfHostError::Ingestion(
                "supplemental missing after store write".to_owned(),
            ));
        };
        let projection_result = (|| {
            let store = self.persistence_lock()?;
            let item_commit =
                apply_supplemental_delta(&mut core, store.as_ref(), &persisted_record, Utc::now())?;
            let manifest = core.manifest_value()?;
            Ok::<_, SelfHostError>((item_commit, manifest))
        })();
        let (item_commit, manifest) = match projection_result {
            Ok(result) => result,
            Err(error) => {
                core.rollback_supplemental(rollback);
                core.mark_non_corpus_materializations_stale();
                return Err(error);
            }
        };
        let commit_result = (|| {
            self.persistence_lock()?
                .commit_supplemental_and_projection(
                    &persisted_record,
                    &ProjectionRef::new("proj:person-page"),
                    &manifest,
                    &item_commit,
                )?;
            Ok::<_, SelfHostError>(())
        })();
        if let Err(error) = commit_result {
            core.rollback_supplemental(rollback);
            core.mark_non_corpus_materializations_stale();
            return Err(error);
        }
        core.activate_non_corpus_projections();

        drop(core);
        self.emit_audit(
            persisted_record.created_by.as_str(),
            AuditEventKind::WriteExecution,
            serde_json::json!({
                "supplemental_id": persisted_record.id.as_str(),
                "kind": persisted_record.kind
            }),
        );

        Ok(persisted_record)
    }

    fn resolve_observation_anchors(
        &self,
        observation_ids: &[ObservationId],
    ) -> Result<HashSet<ObservationId>, SelfHostError> {
        let store = self.persistence_lock()?;
        let mut unresolved = Vec::new();
        let mut resolved = HashSet::new();
        for observation_id in observation_ids {
            match store.observation_by_id(observation_id)? {
                Some(_) => {
                    resolved.insert(observation_id.clone());
                }
                None => unresolved.push(observation_id.as_str().to_owned()),
            }
        }
        if unresolved.is_empty() {
            Ok(resolved)
        } else {
            Err(SelfHostError::SupplementalValidation {
                code: "unresolved_anchor",
                detail: serde_json::json!({
                    "field": "derived_from.observations",
                    "unresolved_observations": unresolved
                }),
            })
        }
    }

    fn resolve_supplemental_anchors(
        &self,
        supplemental_ids: &[SupplementalId],
    ) -> Result<HashMap<SupplementalId, String>, SelfHostError> {
        let store = self.persistence_lock()?;
        let mut unresolved = Vec::new();
        let mut resolved = HashMap::new();
        for supplemental_id in supplemental_ids {
            match store.supplemental_by_id(supplemental_id)? {
                Some(record) => {
                    resolved.insert(supplemental_id.clone(), record.kind);
                }
                None => unresolved.push(supplemental_id.as_str().to_owned()),
            }
        }
        if unresolved.is_empty() {
            Ok(resolved)
        } else {
            Err(SelfHostError::SupplementalValidation {
                code: "unresolved_anchor",
                detail: serde_json::json!({
                    "field": "derived_from.supplementals",
                    "unresolved_supplementals": unresolved
                }),
            })
        }
    }
}

fn validate_supplemental_id(id: &SupplementalId) -> Result<(), SelfHostError> {
    let Some(uuid_part) = id.as_str().strip_prefix("sup:") else {
        return Err(invalid_supplemental_id(id));
    };
    uuid::Uuid::parse_str(uuid_part)
        .map(|_| ())
        .map_err(|_| invalid_supplemental_id(id))
}

fn invalid_supplemental_id(id: &SupplementalId) -> SelfHostError {
    SelfHostError::SupplementalValidation {
        code: "invalid_supplemental_id",
        detail: serde_json::json!({
            "field": "id",
            "actual": id.as_str(),
            "expected": "sup:{uuid}"
        }),
    }
}

fn map_supplemental_kind_error(error: SupplementalKindError) -> SelfHostError {
    match error {
        SupplementalKindError::KindNotRegistered {
            kind,
            major_version,
        }
        | SupplementalKindError::UnregisteredKindPolicyDisabled {
            kind,
            major_version,
        } => SelfHostError::SupplementalValidation {
            code: "kind_not_registered",
            detail: serde_json::json!({
                "kind": kind,
                "major_version": major_version
            }),
        },
        SupplementalKindError::InvalidJsonSchema {
            kind,
            version,
            message,
        } => SelfHostError::SupplementalValidation {
            code: "invalid_payload_schema",
            detail: serde_json::json!({
                "kind": kind,
                "version": version.as_str(),
                "detail": message
            }),
        },
        SupplementalKindError::PayloadSchemaViolation {
            kind,
            major_version,
            violations,
        } => SelfHostError::SupplementalValidation {
            code: "payload_schema_violation",
            detail: serde_json::json!({
                "kind": kind,
                "major_version": major_version,
                "violations": violations
            }),
        },
        SupplementalKindError::MissingRequiredAnchor { kind_ref } => {
            SelfHostError::SupplementalValidation {
                code: "empty_anchor",
                detail: serde_json::json!({
                    "kind": kind_ref,
                    "field": "derived_from",
                    "reason": "kind requires at least one observation, blob, or supplemental anchor"
                }),
            }
        }
        SupplementalKindError::MissingOriginMetadata {
            kind_ref,
            violations,
        } => SelfHostError::SupplementalValidation {
            code: "missing_origin",
            detail: serde_json::json!({
                "kind": kind_ref,
                "field": "payload.origin",
                "violations": violations
            }),
        },
        SupplementalKindError::InvalidKindRef { kind_ref, message } => {
            SelfHostError::SupplementalValidation {
                code: "invalid_supplemental_kind",
                detail: serde_json::json!({
                    "field": "kind",
                    "kind": kind_ref,
                    "reason": message
                }),
            }
        }
        SupplementalKindError::SchemaVersionRuleViolation {
            kind,
            current_version,
            next_version,
            message,
        } => SelfHostError::SupplementalValidation {
            code: "schema_version_rule_violation",
            detail: serde_json::json!({
                "kind": kind,
                "current_version": current_version.as_str(),
                "next_version": next_version.as_str(),
                "reason": message
            }),
        },
        SupplementalKindError::MissingClaimSupplementalAnchor {
            kind_ref,
            referenced_supplementals,
        } => SelfHostError::SupplementalValidation {
            code: "anchor_kind_violation",
            detail: serde_json::json!({
                "kind": kind_ref,
                "field": "derived_from.supplementals",
                "required_kind": "claim@1",
                "ids": referenced_supplementals
            }),
        },
    }
}

fn append_only_conflict(id: &SupplementalId) -> SelfHostError {
    SelfHostError::SupplementalConflict {
        code: "append_only_conflict",
        detail: serde_json::json!({
            "id": id.as_str(),
            "reason": "supplemental id already exists"
        }),
    }
}

fn map_supplemental_store_error(error: lethe_core::domain::DomainError) -> SelfHostError {
    match error {
        lethe_core::domain::DomainError::Policy(policy) if policy.code == "APPEND_ONLY" => {
            SelfHostError::SupplementalConflict {
                code: "append_only_conflict",
                detail: serde_json::json!({
                    "reason": policy.message
                }),
            }
        }
        lethe_core::domain::DomainError::Conflict(message) => SelfHostError::SupplementalConflict {
            code: "append_only_conflict",
            detail: serde_json::json!({
                "reason": message
            }),
        },
        lethe_core::domain::DomainError::Validation(message) => {
            SelfHostError::SupplementalValidation {
                code: "supplemental_store_validation",
                detail: serde_json::json!({
                    "reason": message
                }),
            }
        }
        other => SelfHostError::Ingestion(other.to_string()),
    }
}
