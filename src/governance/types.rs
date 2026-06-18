use serde::{Deserialize, Serialize};

use crate::domain::types::{AuthorityModel, WriteMode};
use crate::domain::values::{ActorRef, EntityRef, ProjectionRef};

// ---------------------------------------------------------------------------
// AccessScope — data classification (M08 §3.1 / §7.1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessScope {
    Public,
    Internal,
    Restricted,
    HighlySensitive,
}

// ---------------------------------------------------------------------------
// Capability — least-privilege tokens (M08 §3.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    ReadRegistry,
    SearchCatalog,
    ReadOwnProjection,
    ReadSharedProjection,
    RunProjectionDraft,
    RequestWritePreview,
    SubmitProposal,
    ApproveProposal,
    ExecuteManagedCanonicalWrite,
    ExecuteSourceNativeWrite,
    ExportData,
    ReadAuditTrail,
}

// ---------------------------------------------------------------------------
// Role — human / agent role (M08 §5.1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    SystemAdmin,
    Researcher,
    Resident,
    External,
    Agent,
}

impl Role {
    /// Capabilities granted to each role in MVP (internal-only).
    pub fn capabilities(&self) -> Vec<Capability> {
        match self {
            Role::SystemAdmin => vec![
                Capability::ReadRegistry,
                Capability::SearchCatalog,
                Capability::ReadOwnProjection,
                Capability::ReadSharedProjection,
                Capability::RunProjectionDraft,
                Capability::RequestWritePreview,
                Capability::SubmitProposal,
                Capability::ApproveProposal,
                Capability::ExecuteManagedCanonicalWrite,
                Capability::ExportData,
                Capability::ReadAuditTrail,
            ],
            Role::Researcher => vec![
                Capability::ReadRegistry,
                Capability::SearchCatalog,
                Capability::ReadOwnProjection,
                Capability::ReadSharedProjection,
                Capability::RunProjectionDraft,
                Capability::RequestWritePreview,
                Capability::SubmitProposal,
            ],
            Role::Resident => vec![Capability::ReadOwnProjection, Capability::SearchCatalog],
            Role::External => vec![Capability::SearchCatalog],
            Role::Agent => vec![
                Capability::ReadRegistry,
                Capability::SearchCatalog,
                Capability::RunProjectionDraft,
                Capability::RequestWritePreview,
                Capability::SubmitProposal,
            ],
        }
    }
}

// ---------------------------------------------------------------------------
// Operation — what the actor wants to do
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Read {
        target: EntityRef,
    },
    Write {
        mode: WriteMode,
        authority: AuthorityModel,
    },
    Export {
        scope: String,
    },
    Publish {
        projection: ProjectionRef,
    },
    RunBuild {
        projection: ProjectionRef,
    },
    ReadRestricted {
        target: EntityRef,
    },
}

// ---------------------------------------------------------------------------
// ConsentStatus — per-entity consent state (M08 §4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsentStatus {
    /// No restriction — internal use allowed.
    Unrestricted,
    /// Restricted capture — filtering required before exposure.
    RestrictedCapture,
    /// Consent explicitly revoked — exclude from projections.
    OptedOut,
}

impl Default for ConsentStatus {
    fn default() -> Self {
        Self::RestrictedCapture
    }
}

// ---------------------------------------------------------------------------
// PolicyRequest — input to the policy engine (M08 §3.4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRequest {
    pub actor: ActorRef,
    pub role: Role,
    pub operation: Operation,
    pub data_scope: AccessScope,
    pub consent_status: ConsentStatus,
    pub environment: Environment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    Sandbox,
    Production,
    Export,
}

// ---------------------------------------------------------------------------
// ReviewRoute — where to send a review task (M08 §6.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewRoute {
    pub reason: String,
    pub triggers: Vec<String>,
}

// ---------------------------------------------------------------------------
// RestrictedFieldSpec — metadata for filtering-before-exposure (M08 §5.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestrictedFieldSpec {
    pub field_path: String,
    pub level: AccessScope,
    pub mask_strategy: MaskStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskStrategy {
    /// Replace with null / placeholder.
    Redact,
    /// Remove the field entirely.
    Exclude,
    /// Hash the value (pseudonymize).
    Hash,
}

// ---------------------------------------------------------------------------
// AuditEvent — governance audit trail (M08 §9)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub actor: ActorRef,
    pub kind: AuditEventKind,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventKind {
    ReadRestricted,
    Export,
    WritePreview,
    WriteExecution,
    Publish,
    Approval,
    Rejection,
    SecretRotation,
    Takedown,
    PhysicalDelete,
    PolicyDenial,
}

// ---------------------------------------------------------------------------
// ConfidenceLevel — identity resolution thresholds (M08 §4.5)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceLevel {
    Low,
    Medium,
    High,
}

// ---------------------------------------------------------------------------
// DenyReason — structured denial
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenyReason {
    pub code: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// PolicyOutcome — result of policy evaluation (M08 §3.3)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome")]
pub enum PolicyOutcome {
    Allow,
    Deny { reason: DenyReason },
    RequireReview { route: ReviewRoute },
}

// ---- helpers for writing assertions in test code --------------------------

impl PolicyOutcome {
    pub fn is_allow(&self) -> bool {
        matches!(self, PolicyOutcome::Allow)
    }

    pub fn is_deny(&self) -> bool {
        matches!(self, PolicyOutcome::Deny { .. })
    }

    pub fn is_require_review(&self) -> bool {
        matches!(self, PolicyOutcome::RequireReview { .. })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_scope_round_trips_via_json() {
        for scope in [
            AccessScope::Public,
            AccessScope::Internal,
            AccessScope::Restricted,
            AccessScope::HighlySensitive,
        ] {
            let json = serde_json::to_string(&scope).unwrap();
            let back: AccessScope = serde_json::from_str(&json).unwrap();
            assert_eq!(scope, back);
        }
    }

    #[test]
    fn capability_exhaustive() {
        let caps = Role::SystemAdmin.capabilities();
        assert!(caps.contains(&Capability::ReadRegistry));
        assert!(caps.contains(&Capability::ApproveProposal));
        // Agent cannot approve
        let agent_caps = Role::Agent.capabilities();
        assert!(!agent_caps.contains(&Capability::ApproveProposal));
    }

    #[test]
    fn role_capabilities_are_subset_chain() {
        let admin = Role::SystemAdmin.capabilities();
        let researcher = Role::Researcher.capabilities();
        // Researcher caps should be subset of admin caps
        for cap in &researcher {
            assert!(admin.contains(cap), "Researcher cap {cap:?} not in Admin");
        }
    }

    #[test]
    fn default_consent_is_restricted() {
        assert_eq!(ConsentStatus::default(), ConsentStatus::RestrictedCapture);
    }

    #[test]
    fn policy_outcome_helpers() {
        assert!(PolicyOutcome::Allow.is_allow());
        assert!(!PolicyOutcome::Allow.is_deny());

        let deny = PolicyOutcome::Deny {
            reason: DenyReason {
                code: "test".into(),
                message: "msg".into(),
            },
        };
        assert!(deny.is_deny());
    }

    #[test]
    fn audit_event_kind_round_trips() {
        for kind in [
            AuditEventKind::ReadRestricted,
            AuditEventKind::Export,
            AuditEventKind::WriteExecution,
            AuditEventKind::PolicyDenial,
        ] {
            let json = serde_json::to_string(&kind).unwrap();
            let back: AuditEventKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, back);
        }
    }

    #[test]
    fn confidence_level_ordering() {
        assert!(ConfidenceLevel::Low < ConfidenceLevel::Medium);
        assert!(ConfidenceLevel::Medium < ConfidenceLevel::High);
    }
}
