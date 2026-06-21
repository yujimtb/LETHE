//! M03 Observation Lake — Ingestion Gate
//!
//! The pipeline that validates, deduplicates, and appends observations.
//! Enforces: L1 (Append-Only), L4 (Explicit Authority), L8 (Idempotency),
//! L11 (Temporal Ordering).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::{
    ActorRef, AuthorityModel, BlobRef, CaptureModel, EntityRef, IdempotencyKey, IngestResult,
    MAX_CLOCK_SKEW, Observation, ObserverRef, QuarantineTicket, SchemaRef, SemVer, SourceSystemRef,
};
use crate::governance::engine::PolicyEngine;
use crate::governance::types::{
    AccessScope, ConsentStatus, Environment, Operation, PolicyOutcome, PolicyRequest, Role,
};
use crate::registry::RegistryStore;

use super::blob::BlobStore;
use super::store::{AppendOutcome, LakeStore};

/// Client-facing request to ingest an observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRequest {
    pub schema: SchemaRef,
    pub schema_version: SemVer,
    pub observer: ObserverRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_system: Option<SourceSystemRef>,
    pub authority_model: AuthorityModel,
    pub capture_model: CaptureModel,
    pub subject: EntityRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<EntityRef>,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub attachments: Vec<BlobRef>,
    pub published: DateTime<Utc>,
    pub idempotency_key: IdempotencyKey,
    #[serde(default)]
    pub meta: serde_json::Value,
}

/// The Ingestion Gate coordinates validation → dedup → append.
pub struct IngestionGate<'a> {
    pub registry: &'a RegistryStore,
    pub lake: &'a mut LakeStore,
    pub blobs: &'a BlobStore,
}

impl IngestionGate<'_> {
    /// Run the full ingestion pipeline (steps 1–9 from the spec).
    pub fn ingest(&mut self, req: IngestRequest) -> IngestResult {
        let obs = match self.prepare_observation(req) {
            Ok(obs) => obs,
            Err(result) => return result,
        };
        let recorded_at = obs.recorded_at;

        match self.lake.append_idempotent(obs) {
            AppendOutcome::Appended(id) => IngestResult::Ingested { id, recorded_at },
            AppendOutcome::Duplicate(existing_id) => IngestResult::Duplicate { existing_id },
            AppendOutcome::Conflict(existing_id) => IngestResult::Quarantined {
                ticket: QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    reason: format!(
                        "sha256-collision: existing observation {existing_id} has different canonical_json"
                    ),
                },
            },
        }
    }

    pub fn prepare_observation(&self, req: IngestRequest) -> Result<Observation, IngestResult> {
        let recorded_at = Utc::now();

        // Step 1: Authenticate observer (must be registered).
        let Some(observer) = self.registry.get_observer(&req.observer) else {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!("Observer {} not registered", req.observer),
            });
        };

        // Step 2: Resolve source contract — verify schema exists.
        let Some(schema) = self.registry.get_schema(&req.schema) else {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!("Schema {} not registered", req.schema),
            });
        };

        let Some(source_system) = req.source_system.as_ref() else {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!(
                    "Observer {} requires source system {}",
                    observer.id, observer.source_system
                ),
            });
        };

        if *source_system != observer.source_system {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!(
                    "Observer {} is bound to source system {}, not {}",
                    observer.id, observer.source_system, source_system
                ),
            });
        }

        let observer_allows_schema = observer.schemas.is_empty()
            || observer
                .schemas
                .iter()
                .any(|schema_ref| schema_ref.as_str() == "*" || *schema_ref == req.schema);
        if !observer_allows_schema {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!("Observer {} cannot emit schema {}", observer.id, req.schema),
            });
        }

        let is_heartbeat = req.schema.as_str() == "schema:observer-heartbeat";
        let expected_authority = if is_heartbeat {
            AuthorityModel::LakeAuthoritative
        } else {
            observer.authority_model
        };
        if req.authority_model != expected_authority {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!(
                    "Observer {} must use authority model {:?}, not {:?}",
                    observer.id, expected_authority, req.authority_model
                ),
            });
        }

        let expected_capture = if is_heartbeat {
            CaptureModel::Event
        } else {
            observer.capture_model
        };
        if req.capture_model != expected_capture {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!(
                    "Observer {} must use capture model {:?}, not {:?}",
                    observer.id, expected_capture, req.capture_model
                ),
            });
        }

        if !schema.source_contracts.is_empty()
            && !schema
                .source_contracts
                .iter()
                .any(|contract| contract.observer_id == observer.id)
        {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message: format!(
                    "Schema {} does not allow observer {}",
                    req.schema, observer.id
                ),
            });
        }

        // Step 3: Validate payload (JSON Schema).
        if let Err(message) = validate_payload(&schema.payload_schema, &req.payload) {
            return Err(IngestResult::Rejected {
                class: crate::domain::FailureClass::ValidationFailure,
                message,
            });
        }

        // Step 4: Governance policy before append.
        let policy = PolicyEngine::evaluate(&PolicyRequest {
            actor: ActorRef::new(req.observer.as_str().replace("obs:", "actor:")),
            role: Role::SystemAdmin,
            operation: Operation::Write {
                mode: crate::domain::WriteMode::Canonical,
                authority: req.authority_model,
            },
            data_scope: AccessScope::Internal,
            consent_status: ConsentStatus::RestrictedCapture,
            environment: Environment::Production,
        });
        if let PolicyOutcome::Deny { reason } = policy {
            return Err(IngestResult::Quarantined {
                ticket: QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    reason: format!("policy denied: {}: {}", reason.code, reason.message),
                },
            });
        }

        // Step 5: Idempotency check is handled by LakeStore.append().

        // Step 6: Verify blob refs exist (if any).
        for br in &req.attachments {
            if !self.blobs.contains(br) {
                return Err(IngestResult::Rejected {
                    class: crate::domain::FailureClass::ValidationFailure,
                    message: format!("Blob {} not found in blob store", br),
                });
            }
        }

        // Step 7 & 8: Temporal validation (L11).
        if req.published > recorded_at + MAX_CLOCK_SKEW {
            return Err(IngestResult::Quarantined {
                ticket: QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    reason: format!(
                        "published ({}) is too far in the future vs recordedAt ({})",
                        req.published, recorded_at
                    ),
                },
            });
        }

        // Step 9: Build Observation and append.
        let obs = Observation {
            id: Observation::new_id(),
            schema: req.schema,
            schema_version: req.schema_version,
            observer: req.observer,
            source_system: req.source_system,
            actor: None,
            authority_model: req.authority_model,
            capture_model: req.capture_model,
            subject: req.subject,
            target: req.target,
            payload: req.payload,
            attachments: req.attachments,
            published: req.published,
            recorded_at,
            consent: None,
            idempotency_key: req.idempotency_key,
            meta: req.meta,
        };

        Ok(obs)
    }
}

fn validate_payload(schema: &serde_json::Value, payload: &serde_json::Value) -> Result<(), String> {
    let validator = jsonschema::validator_for(schema)
        .map_err(|err| format!("Invalid payload schema: {err}"))?;
    validator
        .validate(payload)
        .map_err(|err| format!("Payload does not match schema: {err}"))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    use crate::registry::*;

    /// Helper: build a registry with Slack source + observer + schema.
    fn setup_registry() -> RegistryStore {
        let mut reg = RegistryStore::new();
        reg.register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:slack"),
            name: "Slack".into(),
            provider: Some("Slack".into()),
            api_version: Some("v1".into()),
            source_class: SourceClass::MutableText,
        })
        .unwrap();
        reg.register_observer(Observer {
            id: ObserverRef::new("obs:slack-crawler"),
            name: "Slack Crawler".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:slack"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:slack-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
        reg.register_schema(ObservationSchema {
            id: SchemaRef::new("schema:slack-message"),
            name: "Slack Message".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:message"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        })
        .unwrap();
        reg
    }

    fn valid_request() -> IngestRequest {
        let canonical_json = serde_json::json!({
            "source": "slack",
            "object_id": "channel:C01:ts:999",
            "body": "hello"
        })
        .to_string();
        IngestRequest {
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("message:slack:C01-999"),
            target: None,
            payload: serde_json::json!({"text": "hello"}),
            attachments: vec![],
            published: Utc::now(),
            idempotency_key: IdempotencyKey::new("slack:C01:999"),
            meta: serde_json::json!({
                "canonical_json": canonical_json,
            }),
        }
    }

    #[test]
    fn valid_observation_ingested() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let result = gate.ingest(valid_request());
        assert!(matches!(result, IngestResult::Ingested { .. }));
        assert_eq!(lake.len(), 1);
    }

    #[test]
    fn duplicate_idempotency_key_returns_duplicate() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();

        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };
        gate.ingest(valid_request());

        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };
        let result = gate.ingest(valid_request());
        assert!(matches!(result, IngestResult::Duplicate { .. }));
        assert_eq!(lake.len(), 1);
    }

    #[test]
    fn unregistered_observer_rejected() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.observer = ObserverRef::new("obs:unknown");
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn unregistered_schema_rejected() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.schema = SchemaRef::new("schema:nonexistent");
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn mismatched_source_system_rejected() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.source_system = Some(SourceSystemRef::new("sys:google-slides"));
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn schema_not_authorized_for_observer_rejected() {
        let mut reg = setup_registry();
        reg.register_schema(ObservationSchema {
            id: SchemaRef::new("schema:other"),
            name: "Other".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:message"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        })
        .unwrap();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.schema = SchemaRef::new("schema:other");
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn mismatched_authority_model_rejected() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.authority_model = AuthorityModel::SourceAuthoritative;
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn mismatched_capture_model_rejected() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.capture_model = CaptureModel::Snapshot;
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn schema_source_contract_rejected_for_wrong_observer() {
        let mut reg = RegistryStore::new();
        reg.register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:slack"),
            name: "Slack".into(),
            provider: Some("Slack".into()),
            api_version: Some("v1".into()),
            source_class: SourceClass::MutableText,
        })
        .unwrap();
        reg.register_observer(Observer {
            id: ObserverRef::new("obs:slack-crawler"),
            name: "Slack Crawler".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:slack"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:slack-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
        reg.register_schema(ObservationSchema {
            id: SchemaRef::new("schema:slack-message"),
            name: "Slack Message".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:message"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![SchemaSourceContract {
                observer_id: ObserverRef::new("obs:other"),
                adapter_version: SemVer::new("1.0.0"),
                compatible_range: ">=1.0.0 <2.0.0".into(),
            }],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        })
        .unwrap();

        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let result = gate.ingest(valid_request());
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn invalid_payload_rejected() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.payload = serde_json::json!("not an object");
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn future_published_quarantined() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.published = Utc::now() + chrono::TimeDelta::hours(1);
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Quarantined { .. }));
    }

    #[test]
    fn missing_blob_ref_rejected() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.attachments = vec![BlobRef::new("blob:sha256:0000")];
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Rejected { .. }));
    }

    #[test]
    fn blob_ref_present_accepted() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let mut blobs = BlobStore::new();
        let blob_ref = blobs.put(b"attachment data");

        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.attachments = vec![blob_ref];
        let result = gate.ingest(req);
        assert!(matches!(result, IngestResult::Ingested { .. }));
    }

    #[test]
    fn watermark_and_since_incremental() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();

        // Ingest first.
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };
        gate.ingest(valid_request());

        let wm = lake.watermark().unwrap();

        // Ingest second with different key.
        let mut req2 = valid_request();
        req2.idempotency_key = IdempotencyKey::new("slack:C01:1000");
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };
        gate.ingest(req2);

        let delta = lake.since(wm.position);
        assert_eq!(delta.len(), 1);
    }
}
