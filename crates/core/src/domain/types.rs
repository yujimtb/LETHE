//! M01 Domain Kernel — Closed Algebras & Policy/Decision Types
//!
//! Every enum here is **closed**: adding a variant requires an ADR.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// 3.1 Core Modes
// ---------------------------------------------------------------------------

/// Where the authoritative copy of the data lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityModel {
    LakeAuthoritative,
    SourceAuthoritative,
    DualReference,
}

/// How the data was captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureModel {
    Event,
    Snapshot,
    ChunkManifest,
    Restricted,
}

/// Read-side mode that determines freshness vs reproducibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    AcademicPinned,
    OperationalLatest,
    ApplicationCached,
}

/// Write-side classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Canonical,
    Annotation,
    Proposal,
}

// ---------------------------------------------------------------------------
// 3.2 Storage & Output Kinds
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationKind {
    CanonicalObservation,
    SupplementalRecord,
    GovernanceRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionKind {
    PureProjection,
    CachedProjection,
    WritableProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationKind {
    SqlTables,
    Fileset,
    VectorIndex,
    GraphStore,
    HttpApi,
}

// ---------------------------------------------------------------------------
// 3.3 Policy & Decision Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum PolicyDecision {
    Allow,
    Deny { error: PolicyError },
    RequireReview { task: ReviewTask },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewTask {
    pub id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus {
    Draft,
    PendingReview,
    Approved,
    Rejected,
    Superseded,
}

// ---------------------------------------------------------------------------
// 3.4 Failure Classes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    ValidationFailure,
    PolicyFailure,
    ConflictFailure,
    DeterminismFailure,
    RetryableEffectFailure,
    NonRetryableEffectFailure,
    QuarantineFailure,
}

// ---------------------------------------------------------------------------
// Supplemental Mutability
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mutability {
    AppendOnly,
    ManagedCache,
}

// ---------------------------------------------------------------------------
// Observer / Source types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverType {
    Crawler,
    Connector,
    Bot,
    SensorGateway,
    Human,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    Automated,
    HumanVerified,
    Crowdsourced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceClass {
    MutableMultimodal,
    MutableText,
    ImmutableMultimodal,
    ImmutableText,
}

// ---------------------------------------------------------------------------
// Projection Catalog Status / Health
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionStatus {
    Building,
    Active,
    Stale,
    Degraded,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionHealth {
    Healthy,
    Stale,
    Degraded,
    Broken,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_model_round_trips_via_json() {
        for variant in [
            AuthorityModel::LakeAuthoritative,
            AuthorityModel::SourceAuthoritative,
            AuthorityModel::DualReference,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: AuthorityModel = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back);
        }
    }

    #[test]
    fn capture_model_round_trips_via_json() {
        for variant in [
            CaptureModel::Event,
            CaptureModel::Snapshot,
            CaptureModel::ChunkManifest,
            CaptureModel::Restricted,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let back: CaptureModel = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back);
        }
    }

    #[test]
    fn failure_class_exhaustive() {
        // Ensure all variants compile in a match (exhaustive check).
        let f = FailureClass::ValidationFailure;
        match f {
            FailureClass::ValidationFailure => {}
            FailureClass::PolicyFailure => {}
            FailureClass::ConflictFailure => {}
            FailureClass::DeterminismFailure => {}
            FailureClass::RetryableEffectFailure => {}
            FailureClass::NonRetryableEffectFailure => {}
            FailureClass::QuarantineFailure => {}
        }
    }

    #[test]
    fn policy_decision_deny_json() {
        let d = PolicyDecision::Deny {
            error: PolicyError {
                code: "NO_CONSENT".into(),
                message: "Subject has not consented".into(),
            },
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: PolicyDecision = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }
}
