//! M09 — Retry / backoff policy
//!
//! Pure decision logic: given an error and attempt count, decide whether
//! to retry and how long to wait.  No actual sleeping here.

use std::time::Duration;

use super::config::{BackoffStrategy, RetryConfig};
use super::error::AdapterError;

/// The decision from `should_retry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry after the given duration.
    RetryAfter(Duration),
    /// Do not retry.
    GiveUp { reason: String },
}

/// Evaluate whether a failed attempt should be retried.
pub fn should_retry(error: &AdapterError, attempt: u32, config: &RetryConfig) -> RetryDecision {
    if !error.is_retryable() {
        return RetryDecision::GiveUp {
            reason: format!("non-retryable: {error}"),
        };
    }

    if attempt >= config.max_retries {
        return RetryDecision::GiveUp {
            reason: format!("max retries ({}) exceeded", config.max_retries),
        };
    }

    // If the source told us how long to wait (rate limit), honour it.
    if let AdapterError::RateLimited { retry_after_secs } = error {
        let wait = Duration::from_secs(*retry_after_secs);
        return if wait <= config.max_wait {
            RetryDecision::RetryAfter(wait)
        } else {
            RetryDecision::GiveUp {
                reason: format!(
                    "retry-after {}s exceeds max_wait {}s",
                    retry_after_secs,
                    config.max_wait.as_secs()
                ),
            }
        };
    }

    let wait = compute_backoff(attempt, config);
    if wait <= config.max_wait {
        RetryDecision::RetryAfter(wait)
    } else {
        RetryDecision::GiveUp {
            reason: format!(
                "backoff {}s exceeds max_wait {}s",
                wait.as_secs(),
                config.max_wait.as_secs()
            ),
        }
    }
}

fn compute_backoff(attempt: u32, config: &RetryConfig) -> Duration {
    let base_secs: u64 = match config.backoff {
        BackoffStrategy::Exponential => 1u64.checked_shl(attempt).unwrap_or(u64::MAX),
        BackoffStrategy::Linear => (attempt as u64 + 1) * 2,
        BackoffStrategy::Constant => 2,
    };
    Duration::from_secs(base_secs.min(config.max_wait.as_secs()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RetryConfig {
        RetryConfig {
            max_retries: 3,
            backoff: BackoffStrategy::Exponential,
            max_wait: Duration::from_secs(30),
        }
    }

    #[test]
    fn non_retryable_gives_up_immediately() {
        let err = AdapterError::AuthFailure {
            message: "bad token".into(),
        };
        let decision = should_retry(&err, 0, &test_config());
        assert!(matches!(decision, RetryDecision::GiveUp { .. }));
    }

    #[test]
    fn retryable_error_retries_with_backoff() {
        let err = AdapterError::Network {
            message: "timeout".into(),
        };
        let cfg = test_config();
        let d = should_retry(&err, 0, &cfg);
        assert_eq!(d, RetryDecision::RetryAfter(Duration::from_secs(1)));

        let d = should_retry(&err, 1, &cfg);
        assert_eq!(d, RetryDecision::RetryAfter(Duration::from_secs(2)));

        let d = should_retry(&err, 2, &cfg);
        assert_eq!(d, RetryDecision::RetryAfter(Duration::from_secs(4)));
    }

    #[test]
    fn exceeding_max_retries_gives_up() {
        let err = AdapterError::Network {
            message: "timeout".into(),
        };
        let d = should_retry(&err, 3, &test_config());
        assert!(matches!(d, RetryDecision::GiveUp { .. }));
    }

    #[test]
    fn rate_limit_honours_retry_after() {
        let err = AdapterError::RateLimited {
            retry_after_secs: 5,
        };
        let d = should_retry(&err, 0, &test_config());
        assert_eq!(d, RetryDecision::RetryAfter(Duration::from_secs(5)));
    }

    #[test]
    fn rate_limit_too_long_gives_up() {
        let err = AdapterError::RateLimited {
            retry_after_secs: 999,
        };
        let d = should_retry(&err, 0, &test_config());
        assert!(matches!(d, RetryDecision::GiveUp { .. }));
    }
}
