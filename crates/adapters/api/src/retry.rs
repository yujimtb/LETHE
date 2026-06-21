//! M09 — Retry / backoff policy
//!
//! Pure decision logic: given an error and attempt count, decide whether
//! to retry and how long to wait.  No actual sleeping here.

use std::time::Duration;
use std::{collections::HashMap, sync::Mutex, time::Instant};

use super::config::{BackoffStrategy, RateLimitConfig, RetryConfig};
use super::error::AdapterError;

/// The decision from `should_retry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// Retry after the given duration.
    RetryAfter(Duration),
    /// Do not retry.
    GiveUp { reason: String },
}

#[derive(Debug, Clone)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

#[derive(Debug)]
pub struct ResilientExecutor {
    circuits: Mutex<HashMap<String, CircuitState>>,
    next_request_at: Mutex<HashMap<String, Instant>>,
    failure_threshold: u32,
    open_duration: Duration,
}

impl ResilientExecutor {
    pub fn new(failure_threshold: u32, open_duration: Duration) -> Self {
        assert!(failure_threshold > 0, "failure_threshold must be positive");
        assert!(!open_duration.is_zero(), "open_duration must be positive");
        Self {
            circuits: Mutex::new(HashMap::new()),
            next_request_at: Mutex::new(HashMap::new()),
            failure_threshold,
            open_duration,
        }
    }

    pub fn execute<T, F>(
        &self,
        circuit_key: &str,
        config: &RetryConfig,
        rate_limit: &RateLimitConfig,
        mut operation: F,
    ) -> Result<T, AdapterError>
    where
        F: FnMut() -> Result<T, AdapterError>,
    {
        self.ensure_closed(circuit_key)?;
        self.throttle(circuit_key, rate_limit)?;
        let mut attempt = 0;
        loop {
            match operation() {
                Ok(value) => {
                    self.record_success(circuit_key);
                    return Ok(value);
                }
                Err(error) => match should_retry(&error, attempt, config) {
                    RetryDecision::RetryAfter(wait) => {
                        self.record_failure(circuit_key);
                        std::thread::sleep(wait);
                        attempt += 1;
                    }
                    RetryDecision::GiveUp { .. } => {
                        self.record_failure(circuit_key);
                        return Err(error);
                    }
                },
            }
        }
    }

    fn throttle(&self, key: &str, rate_limit: &RateLimitConfig) -> Result<(), AdapterError> {
        if rate_limit.requests_per_second == 0 || rate_limit.burst == 0 {
            return Err(AdapterError::Other(
                "rate limit configuration must be positive".to_owned(),
            ));
        }
        let interval = Duration::from_secs_f64(1.0 / rate_limit.requests_per_second as f64);
        let mut next = self
            .next_request_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        if let Some(next_at) = next.get(key).copied()
            && next_at > now
        {
            std::thread::sleep(next_at - now);
        }
        next.insert(key.to_owned(), Instant::now() + interval);
        Ok(())
    }

    fn ensure_closed(&self, key: &str) -> Result<(), AdapterError> {
        let mut circuits = self
            .circuits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(state) = circuits.get_mut(key) else {
            return Ok(());
        };
        if let Some(open_until) = state.open_until {
            if Instant::now() < open_until {
                return Err(AdapterError::Other(format!("circuit open for {key}")));
            }
            state.open_until = None;
            state.consecutive_failures = 0;
        }
        Ok(())
    }

    fn record_success(&self, key: &str) {
        self.circuits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(key);
    }

    fn record_failure(&self, key: &str) {
        let mut circuits = self
            .circuits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = circuits.entry(key.to_owned()).or_insert(CircuitState {
            consecutive_failures: 0,
            open_until: None,
        });
        state.consecutive_failures += 1;
        if state.consecutive_failures >= self.failure_threshold {
            state.open_until = Some(Instant::now() + self.open_duration);
        }
    }

    pub fn circuit_open(&self, key: &str) -> bool {
        self.circuits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key)
            .and_then(|state| state.open_until)
            .is_some_and(|until| Instant::now() < until)
    }
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

    #[test]
    fn resilient_executor_opens_circuit_after_repeated_failures() {
        let executor = ResilientExecutor::new(2, Duration::from_secs(60));
        let config = RetryConfig {
            max_retries: 0,
            ..test_config()
        };
        let rate_limit = RateLimitConfig {
            requests_per_second: 1000,
            burst: 1,
        };
        for _ in 0..2 {
            let _ = executor.execute::<(), _>("source:test", &config, &rate_limit, || {
                Err(AdapterError::Network {
                    message: "down".to_owned(),
                })
            });
        }
        assert!(executor.circuit_open("source:test"));
        let error = executor
            .execute::<(), _>("source:test", &config, &rate_limit, || Ok(()))
            .unwrap_err();
        assert!(matches!(error, AdapterError::Other(_)));
    }
}
