use crate::domain::types::{AuthorityModel, WriteMode};
use crate::governance::types::*;

// ---------------------------------------------------------------------------
// PolicyEngine — pure decision service (M08 §10)
//
// evaluateRequest(request, context) =
//   request
//   |> classifyOperation
//   |> checkConsentAndScope
//   |> checkCapabilities
//   |> checkReviewRequirement
//   |> emit(Allow | Deny | RequireReview)
// ---------------------------------------------------------------------------

pub struct PolicyEngine;

impl PolicyEngine {
    /// Evaluate a policy request through the full pipeline.
    pub fn evaluate(request: &PolicyRequest) -> PolicyOutcome {
        if let Some(deny) = Self::check_consent(request) {
            return deny;
        }
        if let Some(deny) = Self::check_capability(request) {
            return deny;
        }
        if let Some(review) = Self::check_review_requirement(request) {
            return review;
        }
        PolicyOutcome::Allow
    }

    // ---- pipeline stages --------------------------------------------------

    /// Check consent/restriction status. Opted-out data must be denied.
    fn check_consent(request: &PolicyRequest) -> Option<PolicyOutcome> {
        match request.consent_status {
            ConsentStatus::OptedOut => Some(PolicyOutcome::Deny {
                reason: DenyReason {
                    code: "consent_opted_out".into(),
                    message: "Subject has opted out — access denied".into(),
                },
            }),
            _ => None,
        }
    }

    /// Check if the actor's role grants the required capability.
    fn check_capability(request: &PolicyRequest) -> Option<PolicyOutcome> {
        let required = Self::required_capability(&request.operation);
        let actor_caps = request.role.capabilities();
        if actor_caps.contains(&required) {
            None
        } else {
            Some(PolicyOutcome::Deny {
                reason: DenyReason {
                    code: "insufficient_capability".into(),
                    message: format!("Role {:?} lacks capability {:?}", request.role, required),
                },
            })
        }
    }

    /// Determine if the operation requires manual review (M08 §6.2).
    fn check_review_requirement(request: &PolicyRequest) -> Option<PolicyOutcome> {
        let mut triggers = Vec::new();

        // HighlySensitive data export always needs review
        if matches!(request.operation, Operation::Export { .. })
            && request.data_scope == AccessScope::HighlySensitive
        {
            triggers.push("high_sensitivity_export".to_string());
        }

        // Canonical write without stable anchor (DualReference)
        if let Operation::Write {
            mode: WriteMode::Canonical,
            authority: AuthorityModel::DualReference,
        } = &request.operation
        {
            triggers.push("dual_reference_canonical_write".to_string());
        }

        // Export environment for restricted data
        if request.environment == Environment::Export
            && request.data_scope == AccessScope::Restricted
        {
            triggers.push("restricted_data_export".to_string());
        }

        // Publish always needs review
        if matches!(request.operation, Operation::Publish { .. }) {
            triggers.push("publication_review".to_string());
        }

        if triggers.is_empty() {
            None
        } else {
            let reason = triggers.join(", ");
            Some(PolicyOutcome::RequireReview {
                route: ReviewRoute {
                    reason: format!("Review required: {reason}"),
                    triggers,
                },
            })
        }
    }

    /// Map an operation to the capability it requires.
    fn required_capability(operation: &Operation) -> Capability {
        match operation {
            Operation::Read { .. } => Capability::ReadSharedProjection,
            Operation::Write { mode, .. } => match mode {
                WriteMode::Canonical => Capability::ExecuteManagedCanonicalWrite,
                WriteMode::Annotation => Capability::SubmitProposal,
                WriteMode::Proposal => Capability::SubmitProposal,
            },
            Operation::Export { .. } => Capability::ExportData,
            Operation::Publish { .. } => Capability::ApproveProposal,
            Operation::RunBuild { .. } => Capability::RunProjectionDraft,
            Operation::ReadRestricted { .. } => Capability::ReadSharedProjection,
        }
    }

    // ---- identity resolution confidence helpers (M08 §4.5) ----------------

    /// Whether a candidate at the given confidence may be auto-promoted
    /// to `resolved_persons`.
    pub fn may_auto_promote(confidence: ConfidenceLevel) -> bool {
        confidence == ConfidenceLevel::High
    }

    /// Whether the candidate requires manual review before promotion.
    pub fn requires_review_for_promotion(confidence: ConfidenceLevel) -> bool {
        confidence == ConfidenceLevel::Medium
    }

    /// Whether the candidate may be used in published/shared projections.
    pub fn allowed_in_published(confidence: ConfidenceLevel) -> bool {
        confidence == ConfidenceLevel::High
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::values::ActorRef;

    fn make_request(
        role: Role,
        operation: Operation,
        scope: AccessScope,
        consent: ConsentStatus,
        env: Environment,
    ) -> PolicyRequest {
        PolicyRequest {
            actor: ActorRef::new("actor:test"),
            role,
            operation,
            data_scope: scope,
            consent_status: consent,
            environment: env,
        }
    }

    #[test]
    fn allow_internal_read() {
        let req = make_request(
            Role::Researcher,
            Operation::Read {
                target: crate::domain::values::EntityRef::new("person:alice"),
            },
            AccessScope::Internal,
            ConsentStatus::Unrestricted,
            Environment::Production,
        );
        assert!(PolicyEngine::evaluate(&req).is_allow());
    }

    #[test]
    fn deny_opted_out_subject() {
        let req = make_request(
            Role::SystemAdmin,
            Operation::Read {
                target: crate::domain::values::EntityRef::new("person:bob"),
            },
            AccessScope::Internal,
            ConsentStatus::OptedOut,
            Environment::Production,
        );
        let outcome = PolicyEngine::evaluate(&req);
        assert!(outcome.is_deny());
    }

    #[test]
    fn deny_insufficient_capability() {
        let req = make_request(
            Role::External,
            Operation::Read {
                target: crate::domain::values::EntityRef::new("person:x"),
            },
            AccessScope::Internal,
            ConsentStatus::Unrestricted,
            Environment::Production,
        );
        let outcome = PolicyEngine::evaluate(&req);
        assert!(outcome.is_deny());
    }

    #[test]
    fn require_review_for_publication() {
        let req = make_request(
            Role::SystemAdmin,
            Operation::Publish {
                projection: crate::domain::values::ProjectionRef::new("proj:person-page"),
            },
            AccessScope::Internal,
            ConsentStatus::Unrestricted,
            Environment::Production,
        );
        let outcome = PolicyEngine::evaluate(&req);
        assert!(outcome.is_require_review());
    }

    #[test]
    fn require_review_for_highly_sensitive_export() {
        let req = make_request(
            Role::SystemAdmin,
            Operation::Export {
                scope: "full".into(),
            },
            AccessScope::HighlySensitive,
            ConsentStatus::Unrestricted,
            Environment::Export,
        );
        let outcome = PolicyEngine::evaluate(&req);
        assert!(outcome.is_require_review());
    }

    #[test]
    fn require_review_dual_reference_canonical_write() {
        let req = make_request(
            Role::SystemAdmin,
            Operation::Write {
                mode: WriteMode::Canonical,
                authority: AuthorityModel::DualReference,
            },
            AccessScope::Internal,
            ConsentStatus::Unrestricted,
            Environment::Production,
        );
        let outcome = PolicyEngine::evaluate(&req);
        assert!(outcome.is_require_review());
    }

    #[test]
    fn annotation_write_allowed_for_researcher() {
        let req = make_request(
            Role::Researcher,
            Operation::Write {
                mode: WriteMode::Annotation,
                authority: AuthorityModel::LakeAuthoritative,
            },
            AccessScope::Internal,
            ConsentStatus::Unrestricted,
            Environment::Production,
        );
        assert!(PolicyEngine::evaluate(&req).is_allow());
    }

    #[test]
    fn agent_cannot_canonical_write() {
        let req = make_request(
            Role::Agent,
            Operation::Write {
                mode: WriteMode::Canonical,
                authority: AuthorityModel::LakeAuthoritative,
            },
            AccessScope::Internal,
            ConsentStatus::Unrestricted,
            Environment::Sandbox,
        );
        let outcome = PolicyEngine::evaluate(&req);
        assert!(outcome.is_deny());
    }

    #[test]
    fn agent_can_run_build() {
        let req = make_request(
            Role::Agent,
            Operation::RunBuild {
                projection: crate::domain::values::ProjectionRef::new("proj:test"),
            },
            AccessScope::Internal,
            ConsentStatus::Unrestricted,
            Environment::Sandbox,
        );
        assert!(PolicyEngine::evaluate(&req).is_allow());
    }

    #[test]
    fn consent_check_runs_before_capability() {
        // Even admin is denied on opted-out data
        let req = make_request(
            Role::SystemAdmin,
            Operation::Read {
                target: crate::domain::values::EntityRef::new("person:z"),
            },
            AccessScope::Internal,
            ConsentStatus::OptedOut,
            Environment::Production,
        );
        let outcome = PolicyEngine::evaluate(&req);
        assert!(outcome.is_deny());
        if let PolicyOutcome::Deny { reason } = outcome {
            assert_eq!(reason.code, "consent_opted_out");
        }
    }

    // ---- confidence threshold tests (M08 §4.5) ---------------------------

    #[test]
    fn high_confidence_auto_promotes() {
        assert!(PolicyEngine::may_auto_promote(ConfidenceLevel::High));
        assert!(!PolicyEngine::may_auto_promote(ConfidenceLevel::Medium));
        assert!(!PolicyEngine::may_auto_promote(ConfidenceLevel::Low));
    }

    #[test]
    fn medium_confidence_requires_review() {
        assert!(PolicyEngine::requires_review_for_promotion(
            ConfidenceLevel::Medium
        ));
        assert!(!PolicyEngine::requires_review_for_promotion(
            ConfidenceLevel::High
        ));
    }

    #[test]
    fn only_high_allowed_in_published() {
        assert!(PolicyEngine::allowed_in_published(ConfidenceLevel::High));
        assert!(!PolicyEngine::allowed_in_published(ConfidenceLevel::Medium));
        assert!(!PolicyEngine::allowed_in_published(ConfidenceLevel::Low));
    }
}
