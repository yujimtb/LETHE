//! M03 Observation Lake — Ingestion Gate
//!
//! The pipeline that validates, deduplicates, and appends observations.
//! Enforces: L1 (Append-Only), L4 (Explicit Authority), L8 (Idempotency),
//! L11 (Temporal Ordering).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use lethe_core::domain::{
    ActorRef, AuthorityModel, BlobRef, CaptureModel, EntityRef, IdempotencyKey, IngestResult,
    MAX_CLOCK_SKEW, Observation, ObserverRef, QuarantineTicket, SchemaRef, SemVer, SourceSystemRef,
};
use lethe_policy::governance::engine::PolicyEngine;
use lethe_policy::governance::types::{
    AccessScope, ConsentStatus, Environment, Operation, PolicyOutcome, PolicyRequest, Role,
};
use lethe_registry::registry::RegistryStore;

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

pub const COMM_CHANNEL_ID_META_KEY: &str = "communication_channel_id";
pub const COMM_CHANNEL_KIND_META_KEY: &str = "communication_channel_kind";
pub const COMM_CHANNEL_EXTERNAL_ID_META_KEY: &str = "communication_channel_external_id";
pub const COMM_SENDER_ID_META_KEY: &str = "communication_sender_id";
pub const COMM_THREAD_REF_META_KEY: &str = "communication_thread_ref";

/// Validates and normalizes an ingestion request without accessing the lake.
pub struct ObservationPreparer<'a> {
    registry: &'a RegistryStore,
    blobs: &'a BlobStore,
}

impl<'a> ObservationPreparer<'a> {
    pub fn new(registry: &'a RegistryStore, blobs: &'a BlobStore) -> Self {
        Self { registry, blobs }
    }

    /// Validate and normalize a request into an Observation ready for durable append.
    pub fn prepare(&self, mut req: IngestRequest) -> Result<Observation, IngestResult> {
        let recorded_at = Utc::now();

        // Step 1: Authenticate observer (must be registered).
        let Some(observer) = self.registry.get_observer(&req.observer) else {
            return Err(IngestResult::Rejected {
                class: lethe_core::domain::FailureClass::ValidationFailure,
                message: format!("Observer {} not registered", req.observer),
            });
        };

        // Step 2: Resolve source contract — verify schema exists.
        let Some(schema) = self.registry.get_schema(&req.schema) else {
            return Err(IngestResult::Rejected {
                class: lethe_core::domain::FailureClass::ValidationFailure,
                message: format!("Schema {} not registered", req.schema),
            });
        };

        let Some(source_system) = req.source_system.as_ref() else {
            return Err(IngestResult::Rejected {
                class: lethe_core::domain::FailureClass::ValidationFailure,
                message: format!(
                    "Observer {} requires source system {}",
                    observer.id, observer.source_system
                ),
            });
        };

        if *source_system != observer.source_system {
            return Err(IngestResult::Rejected {
                class: lethe_core::domain::FailureClass::ValidationFailure,
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
                class: lethe_core::domain::FailureClass::ValidationFailure,
                message: format!("Observer {} cannot emit schema {}", observer.id, req.schema),
            });
        }

        let is_heartbeat = req.schema.as_str() == "schema:observer-heartbeat";
        let expected_authority = if is_heartbeat {
            AuthorityModel::LakeAuthoritative
        } else {
            observer.authority_model
        };
        if req.authority_model != expected_authority
            && req.authority_model != AuthorityModel::DualReference
        {
            return Err(IngestResult::Rejected {
                class: lethe_core::domain::FailureClass::ValidationFailure,
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
                class: lethe_core::domain::FailureClass::ValidationFailure,
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
                class: lethe_core::domain::FailureClass::ValidationFailure,
                message: format!(
                    "Schema {} does not allow observer {}",
                    req.schema, observer.id
                ),
            });
        }

        // Step 3: Validate payload (JSON Schema).
        if let Err(message) = validate_payload(&schema.payload_schema, &req.payload) {
            return Err(IngestResult::Rejected {
                class: lethe_core::domain::FailureClass::ValidationFailure,
                message,
            });
        }

        // Step 4: Governance policy before append.
        let policy = PolicyEngine::evaluate(&PolicyRequest {
            actor: ActorRef::new(req.observer.as_str().replace("obs:", "actor:")),
            role: Role::SystemAdmin,
            operation: Operation::Write {
                mode: lethe_core::domain::WriteMode::Canonical,
                authority: req.authority_model,
            },
            data_scope: AccessScope::Internal,
            consent_status: ConsentStatus::RestrictedCapture,
            environment: Environment::Production,
        });
        match policy {
            PolicyOutcome::Allow => {}
            PolicyOutcome::Deny { reason } => {
                return Err(IngestResult::Quarantined {
                    ticket: QuarantineTicket {
                        id: uuid::Uuid::now_v7().to_string(),
                        kind: lethe_core::domain::QuarantineKind::Policy,
                        reason: format!("policy denied: {}: {}", reason.code, reason.message),
                    },
                });
            }
            PolicyOutcome::RequireReview { route } => {
                return Err(IngestResult::Quarantined {
                    ticket: QuarantineTicket {
                        id: uuid::Uuid::now_v7().to_string(),
                        kind: lethe_core::domain::QuarantineKind::Policy,
                        reason: format!("policy review required: {}", route.reason),
                    },
                });
            }
        }

        // Step 5: Idempotency is decided by the append boundary.

        // Step 6: Verify blob refs exist (if any).
        for br in &req.attachments {
            if !self.blobs.contains(br) {
                return Err(IngestResult::Rejected {
                    class: lethe_core::domain::FailureClass::ValidationFailure,
                    message: format!("Blob {} not found in blob store", br),
                });
            }
        }

        // Step 7 & 8: Temporal validation (L11).
        if req.published > recorded_at + MAX_CLOCK_SKEW {
            return Err(IngestResult::Quarantined {
                ticket: QuarantineTicket {
                    id: uuid::Uuid::now_v7().to_string(),
                    kind: lethe_core::domain::QuarantineKind::ClockSkewFuture,
                    reason: format!(
                        "published ({}) is too far in the future vs recordedAt ({})",
                        req.published, recorded_at
                    ),
                },
            });
        }

        let consent = match self.apply_channel_context(&mut req) {
            Ok(consent) => consent,
            Err(ticket) => return Err(IngestResult::Quarantined { ticket }),
        };

        // Step 9: Build the Observation. Appending is the caller's responsibility.
        Ok(Observation {
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
            consent,
            idempotency_key: req.idempotency_key,
            meta: req.meta,
        })
    }

    fn apply_channel_context(
        &self,
        req: &mut IngestRequest,
    ) -> Result<Option<lethe_core::domain::ConsentRef>, QuarantineTicket> {
        let Some(source_system) = req.source_system.as_ref() else {
            return Ok(None);
        };
        let Some(kind) = lethe_registry::registry::ChannelKind::from_source_system(source_system)
        else {
            return Ok(None);
        };
        if !is_communication_message_schema(req.schema.as_str()) {
            return Ok(None);
        }

        let mut meta = req.meta.as_object().cloned().unwrap_or_default();
        let source_instance_id = meta
            .get("source_instance")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                channel_quarantine("communication observation missing source_instance")
            })?;
        let external_id = meta
            .get(COMM_CHANNEL_EXTERNAL_ID_META_KEY)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| communication_external_id(kind, &req.payload))
            .ok_or_else(|| {
                channel_quarantine("communication observation missing channel external id")
            })?;
        let channel = if let Some(channel_id) = meta
            .get(COMM_CHANNEL_ID_META_KEY)
            .and_then(serde_json::Value::as_str)
        {
            self.registry.get_channel(channel_id)
        } else {
            self.registry
                .get_channel_by_source(kind, &source_instance_id, &external_id)
        }
        .ok_or_else(|| {
            channel_quarantine(format!(
                "unregistered communication channel: kind={kind} source_instance={source_instance_id} external_id={external_id}"
            ))
        })?;

        if !channel.enabled {
            return Err(channel_quarantine(format!(
                "disabled communication channel: {}",
                channel.id
            )));
        }
        if channel.kind != kind
            || channel.source_instance_id != source_instance_id
            || channel.external_id != external_id
        {
            return Err(channel_quarantine(format!(
                "communication channel {} does not match kind/source_instance/external_id",
                channel.id
            )));
        }

        let sender = meta
            .get(COMM_SENDER_ID_META_KEY)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| communication_sender(kind, &req.payload))
            .ok_or_else(|| channel_quarantine("communication observation missing sender"))?;
        let thread_ref = meta
            .get(COMM_THREAD_REF_META_KEY)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| communication_thread_ref(kind, &req.payload))
            .ok_or_else(|| {
                channel_quarantine("communication observation missing thread context")
            })?;
        let reply_due_at =
            req.published + chrono::TimeDelta::seconds(channel.reply_slo_seconds as i64);

        meta.insert(
            COMM_CHANNEL_ID_META_KEY.to_owned(),
            serde_json::Value::String(channel.id.clone()),
        );
        meta.insert(
            COMM_CHANNEL_KIND_META_KEY.to_owned(),
            serde_json::Value::String(channel.kind.as_str().to_owned()),
        );
        meta.insert(
            COMM_CHANNEL_EXTERNAL_ID_META_KEY.to_owned(),
            serde_json::Value::String(channel.external_id.clone()),
        );
        meta.insert(
            COMM_SENDER_ID_META_KEY.to_owned(),
            serde_json::Value::String(sender.clone()),
        );
        meta.insert(
            COMM_THREAD_REF_META_KEY.to_owned(),
            serde_json::Value::String(thread_ref.clone()),
        );
        meta.insert(
            "communication".to_owned(),
            serde_json::json!({
                "channel_id": channel.id,
                "kind": channel.kind,
                "source_instance": channel.source_instance_id,
                "external_id": channel.external_id,
                "sender": sender,
                "thread_ref": thread_ref,
                "reply_slo_seconds": channel.reply_slo_seconds,
                "reply_due_at": reply_due_at,
                "freshness_threshold_seconds": channel.freshness_threshold_seconds,
                "break_glass_channel": channel.break_glass_channel,
                "break_glass_senders": channel.break_glass_senders,
            }),
        );
        req.meta = serde_json::Value::Object(meta);

        Ok(Some(lethe_core::domain::ConsentRef::new(
            channel.default_consent_scope.clone(),
        )))
    }
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
        let obs = match ObservationPreparer::new(self.registry, self.blobs).prepare(req) {
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
                    kind: lethe_core::domain::QuarantineKind::CanonicalCollision,
                    reason: format!(
                        "sha256-collision: existing observation {existing_id} has different canonical_json"
                    ),
                },
            },
        }
    }
}

fn is_communication_message_schema(schema: &str) -> bool {
    matches!(
        schema,
        "schema:slack-message" | "schema:gmail-message" | "schema:discord-message"
    )
}

fn communication_external_id(
    kind: lethe_registry::registry::ChannelKind,
    payload: &serde_json::Value,
) -> Option<String> {
    let field = match kind {
        lethe_registry::registry::ChannelKind::Slack => "channel_id",
        lethe_registry::registry::ChannelKind::Gmail => "account_id",
        lethe_registry::registry::ChannelKind::Discord => "channel_id",
    };
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn communication_sender(
    kind: lethe_registry::registry::ChannelKind,
    payload: &serde_json::Value,
) -> Option<String> {
    match kind {
        lethe_registry::registry::ChannelKind::Slack => payload
            .get("user_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        lethe_registry::registry::ChannelKind::Gmail => payload
            .get("from")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        lethe_registry::registry::ChannelKind::Discord => payload
            .get("author_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
    }
}

fn communication_thread_ref(
    kind: lethe_registry::registry::ChannelKind,
    payload: &serde_json::Value,
) -> Option<String> {
    match kind {
        lethe_registry::registry::ChannelKind::Slack => payload
            .get("thread_ts")
            .and_then(serde_json::Value::as_str)
            .or_else(|| payload.get("ts").and_then(serde_json::Value::as_str))
            .map(|value| format!("slack:thread:{value}")),
        lethe_registry::registry::ChannelKind::Gmail => payload
            .get("thread_id")
            .and_then(serde_json::Value::as_str)
            .map(|value| format!("gmail:thread:{value}")),
        lethe_registry::registry::ChannelKind::Discord => payload
            .get("referenced_message_id")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                payload
                    .get("message_id")
                    .and_then(serde_json::Value::as_str)
            })
            .map(|value| format!("discord:thread:{value}")),
    }
}

fn channel_quarantine(message: impl Into<String>) -> QuarantineTicket {
    QuarantineTicket {
        id: uuid::Uuid::now_v7().to_string(),
        kind: lethe_core::domain::QuarantineKind::Channel,
        reason: message.into(),
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
    use lethe_core::domain::*;
    use lethe_registry::registry::*;

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
        reg.register_channel(ChannelRecord {
            id: "chan:slack:test:C01".into(),
            kind: ChannelKind::Slack,
            source_instance_id: "slack-test".into(),
            external_id: "C01".into(),
            connection_ref: "source:slack-test".into(),
            default_consent_scope: "personal".into(),
            reply_slo_seconds: 1800,
            freshness_threshold_seconds: 1800,
            break_glass_channel: false,
            break_glass_senders: vec![],
            enabled: true,
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
            payload: serde_json::json!({
                "text": "hello",
                "channel_id": "C01",
                "user_id": "U01",
                "ts": "999.000000",
                "thread_ts": "999.000000",
            }),
            attachments: vec![],
            published: Utc::now(),
            idempotency_key: IdempotencyKey::new("slack:C01:999"),
            meta: serde_json::json!({
                "canonical_json": canonical_json,
                "source_instance": "slack-test",
            }),
        }
    }

    fn comparable_observation(observation: &Observation) -> serde_json::Value {
        let mut value = serde_json::to_value(observation).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("id");
        object.remove("recorded_at");
        value
    }

    #[test]
    fn preparer_and_gate_produce_equivalent_observations() {
        let reg = setup_registry();
        let blobs = BlobStore::new();
        let request = valid_request();
        let prepared = ObservationPreparer::new(&reg, &blobs)
            .prepare(request.clone())
            .unwrap();
        let mut lake = LakeStore::new();
        let result = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        }
        .ingest(request);

        let IngestResult::Ingested { id, recorded_at } = result else {
            panic!("gate must ingest a request accepted by the preparer");
        };
        let appended = &lake.list()[0];
        assert_eq!(id, appended.id);
        assert_eq!(recorded_at, appended.recorded_at);
        assert_eq!(
            comparable_observation(&prepared),
            comparable_observation(appended)
        );
    }

    #[test]
    fn preparer_and_gate_return_the_same_validation_rejection() {
        let reg = setup_registry();
        let blobs = BlobStore::new();
        let mut request = valid_request();
        request.observer = ObserverRef::new("obs:unknown");
        let direct = ObservationPreparer::new(&reg, &blobs)
            .prepare(request.clone())
            .unwrap_err();
        let mut lake = LakeStore::new();
        let gated = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        }
        .ingest(request);

        match (direct, gated) {
            (
                IngestResult::Rejected {
                    class: direct_class,
                    message: direct_message,
                },
                IngestResult::Rejected {
                    class: gated_class,
                    message: gated_message,
                },
            ) => {
                assert_eq!(direct_class, gated_class);
                assert_eq!(direct_message, gated_message);
            }
            other => panic!("preparer and gate must return the same rejection: {other:?}"),
        }
        assert!(lake.is_empty());
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
        assert_eq!(
            lake.list()[0].consent.as_ref().unwrap().as_str(),
            "personal"
        );
        assert_eq!(
            lake.list()[0].meta[COMM_CHANNEL_ID_META_KEY],
            "chan:slack:test:C01"
        );
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
        reg.register_channel(ChannelRecord {
            id: "chan:slack:test:C01".into(),
            kind: ChannelKind::Slack,
            source_instance_id: "slack-test".into(),
            external_id: "C01".into(),
            connection_ref: "source:slack-test".into(),
            default_consent_scope: "personal".into(),
            reply_slo_seconds: 1800,
            freshness_threshold_seconds: 1800,
            break_glass_channel: false,
            break_glass_senders: vec![],
            enabled: true,
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
    fn unregistered_communication_channel_is_quarantined() {
        let reg = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };

        let mut req = valid_request();
        req.payload["channel_id"] = serde_json::json!("C99");

        let result = gate.ingest(req);

        assert!(matches!(result, IngestResult::Quarantined { .. }));
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
        req2.subject = EntityRef::new("message:slack:C01-1000");
        req2.payload["ts"] = serde_json::json!("1000.000000");
        req2.payload["thread_ts"] = serde_json::json!("1000.000000");
        req2.meta["canonical_json"] = serde_json::json!({
            "source": "slack",
            "object_id": "channel:C01:ts:1000",
            "body": "hello"
        })
        .to_string()
        .into();
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
