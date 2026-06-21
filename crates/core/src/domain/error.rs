//! M01 Domain Kernel — Error types mapped to FailureClass

use super::types::{FailureClass, PolicyError, ReviewTask};
use super::values::ObservationId;

/// The result of an ingestion attempt.
#[derive(Debug, Clone)]
pub enum IngestResult {
    Ingested {
        id: ObservationId,
        recorded_at: chrono::DateTime<chrono::Utc>,
    },
    Duplicate {
        existing_id: ObservationId,
    },
    Rejected {
        class: FailureClass,
        message: String,
    },
    Quarantined {
        ticket: QuarantineTicket,
    },
}

#[derive(Debug, Clone)]
pub struct QuarantineTicket {
    pub id: String,
    pub reason: String,
}

/// Domain-level error — used internally; not an HTTP error.
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("validation: {0}")]
    Validation(String),

    #[error("policy: {}", .0.message)]
    Policy(PolicyError),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("review required: {}", .0.reason)]
    ReviewRequired(ReviewTask),

    #[error("quarantine: {0}")]
    Quarantine(String),

    #[error("not found: {0}")]
    NotFound(String),
}

impl DomainError {
    pub fn failure_class(&self) -> FailureClass {
        match self {
            Self::Validation(_) => FailureClass::ValidationFailure,
            Self::Policy(_) => FailureClass::PolicyFailure,
            Self::Conflict(_) => FailureClass::ConflictFailure,
            Self::ReviewRequired(_) => FailureClass::QuarantineFailure,
            Self::Quarantine(_) => FailureClass::QuarantineFailure,
            Self::NotFound(_) => FailureClass::ValidationFailure,
        }
    }
}
