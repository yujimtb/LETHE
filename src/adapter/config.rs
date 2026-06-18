//! M09 — AdapterConfig

use crate::domain::{
    AuthorityModel, CaptureModel, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Schema binding: which schema version range an adapter supports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaBinding {
    pub schema: SchemaRef,
    /// SemVer range string, e.g. ">=1.0.0 <2.0.0"
    pub versions: String,
}

/// Rate-limit configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub burst: u32,
}

/// Retry configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub backoff: BackoffStrategy,
    #[serde(with = "humantime_serde_compat")]
    pub max_wait: Duration,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    Exponential,
    Linear,
    Constant,
}

/// Common adapter configuration (M09 §3.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterConfig {
    pub observer_id: ObserverRef,
    pub source_system_id: SourceSystemRef,
    pub adapter_version: SemVer,
    pub authority_model: AuthorityModel,
    pub capture_model: CaptureModel,
    pub schemas: Vec<SchemaRef>,
    pub schema_bindings: Vec<SchemaBinding>,
    #[serde(with = "humantime_serde_compat")]
    pub poll_interval: Duration,
    #[serde(with = "humantime_serde_compat")]
    pub heartbeat_interval: Duration,
    pub rate_limit: RateLimitConfig,
    pub retry: RetryConfig,
    /// Opaque reference to a credential secret. Actual credential
    /// retrieval is out of scope (MVP: config interface only).
    pub credential_ref: String,
}

/// Simple Duration serialization as seconds.
mod humantime_serde_compat {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(d: &Duration, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}
