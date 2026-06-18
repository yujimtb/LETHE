use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

// ---------------------------------------------------------------------------
// RuntimeConfig — top-level runtime configuration (M15 §6, §7)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub environment: RuntimeEnvironment,
    pub build: BuildConfig,
    pub health: HealthConfig,
    pub storage: StorageConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            environment: RuntimeEnvironment::Local,
            build: BuildConfig::default(),
            health: HealthConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// RuntimeEnvironment
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEnvironment {
    Local,
    Ci,
    Container,
}

// ---------------------------------------------------------------------------
// BuildConfig — build isolation / sandbox rules (M15 §6)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildConfig {
    /// Network policy: default deny for builds.
    pub network_policy: NetworkPolicy,
    /// Maximum build duration.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Max memory in bytes (0 = unlimited for MVP local).
    pub max_memory_bytes: u64,
    /// Working directory for build artifacts.
    pub work_dir: PathBuf,
    /// Whether to record build image digest.
    pub record_image_digest: bool,
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            network_policy: NetworkPolicy::DefaultDeny,
            timeout: Duration::from_secs(300),
            max_memory_bytes: 0,
            work_dir: PathBuf::from("./build"),
            record_image_digest: false,
        }
    }
}

// ---------------------------------------------------------------------------
// NetworkPolicy — sandbox network isolation (M15 §6.1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    /// No outbound network (recommended for builds).
    DefaultDeny,
    /// Allow specific hosts (growth phase).
    AllowList,
    /// Unrestricted (dev/test only — violates sandbox principle).
    Unrestricted,
}

// ---------------------------------------------------------------------------
// HealthConfig — observer health / heartbeat thresholds (M15 §8.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Default heartbeat interval expected from observers.
    #[serde(with = "humantime_serde")]
    pub default_heartbeat_interval: Duration,
    /// Default maximum gap before alert.
    #[serde(with = "humantime_serde")]
    pub default_max_gap: Duration,
    /// Per-observer overrides keyed by observer ref string.
    pub observer_overrides: HashMap<String, ObserverHealthThreshold>,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            default_heartbeat_interval: Duration::from_secs(60),
            default_max_gap: Duration::from_secs(300),
            observer_overrides: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserverHealthThreshold {
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub max_gap: Duration,
}

// ---------------------------------------------------------------------------
// StorageConfig — MVP storage paths (M15 §7.1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub lake_path: PathBuf,
    pub blob_path: PathBuf,
    pub registry_path: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            lake_path: PathBuf::from("./data/lake"),
            blob_path: PathBuf::from("./data/blobs"),
            registry_path: PathBuf::from("./data/registry"),
        }
    }
}

// ---------------------------------------------------------------------------
// humantime_serde — Duration ↔ human-readable string ("5m", "300s")
// ---------------------------------------------------------------------------

mod humantime_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_sane_values() {
        let cfg = RuntimeConfig::default();
        assert_eq!(cfg.environment, RuntimeEnvironment::Local);
        assert_eq!(cfg.build.network_policy, NetworkPolicy::DefaultDeny);
        assert_eq!(cfg.build.timeout, Duration::from_secs(300));
        assert_eq!(cfg.health.default_max_gap, Duration::from_secs(300));
    }

    #[test]
    fn config_round_trips_via_json() {
        let cfg = RuntimeConfig::default();
        let json = serde_json::to_string_pretty(&cfg).unwrap();
        let back: RuntimeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.environment, cfg.environment);
        assert_eq!(back.build.network_policy, cfg.build.network_policy);
        assert_eq!(back.build.timeout, cfg.build.timeout);
    }

    #[test]
    fn network_policy_variants() {
        for policy in [
            NetworkPolicy::DefaultDeny,
            NetworkPolicy::AllowList,
            NetworkPolicy::Unrestricted,
        ] {
            let json = serde_json::to_string(&policy).unwrap();
            let back: NetworkPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(policy, back);
        }
    }

    #[test]
    fn observer_health_override() {
        let mut cfg = HealthConfig::default();
        cfg.observer_overrides.insert(
            "obs:slack-crawler".into(),
            ObserverHealthThreshold {
                heartbeat_interval: Duration::from_secs(30),
                max_gap: Duration::from_secs(120),
            },
        );
        let json = serde_json::to_string(&cfg).unwrap();
        let back: HealthConfig = serde_json::from_str(&json).unwrap();
        let override_val = back.observer_overrides.get("obs:slack-crawler").unwrap();
        assert_eq!(override_val.max_gap, Duration::from_secs(120));
    }
}
