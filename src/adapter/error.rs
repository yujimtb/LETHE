//! M09 — Adapter error types mapped to FailureClass

use crate::domain::FailureClass;

/// Errors specific to the adapter layer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AdapterError {
    /// Rate limited — should backoff and retry.
    #[error("rate limited: retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },

    /// Authentication / authorisation failure — stop and alert.
    #[error("auth failure: {message}")]
    AuthFailure { message: String },

    /// Transient network / timeout — retry.
    #[error("network: {message}")]
    Network { message: String },

    /// Source returned malformed / unparsable data — quarantine.
    #[error("malformed response: {message}")]
    MalformedResponse { message: String },

    /// Partial batch failure — some items succeeded, some failed.
    #[error("partial failure: {succeeded} ok, {failed} failed")]
    PartialFailure { succeeded: usize, failed: usize },

    /// Generic non-retryable error.
    #[error("adapter error: {0}")]
    Other(String),
}

impl AdapterError {
    pub fn failure_class(&self) -> FailureClass {
        match self {
            Self::RateLimited { .. } | Self::Network { .. } => FailureClass::RetryableEffectFailure,
            Self::AuthFailure { .. } | Self::Other(_) => FailureClass::NonRetryableEffectFailure,
            Self::MalformedResponse { .. } => FailureClass::QuarantineFailure,
            Self::PartialFailure { .. } => FailureClass::RetryableEffectFailure,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.failure_class(), FailureClass::RetryableEffectFailure)
    }
}
