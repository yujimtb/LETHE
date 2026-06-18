use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::runtime::config::BuildConfig;

// ---------------------------------------------------------------------------
// BuildRunner trait — abstraction for build execution (M15 §4.2)
// ---------------------------------------------------------------------------

/// Outcome of a build execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildOutcome {
    pub success: bool,
    pub artifact_hash: Option<String>,
    pub duration: Duration,
    pub log: String,
    pub timed_out: bool,
}

/// Trait for executing projection builds with isolation.
pub trait BuildRunner: Send + Sync {
    fn run(&self, spec: &BuildSpec) -> BuildOutcome;
}

/// Input to a build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildSpec {
    pub projection_id: String,
    pub entrypoint: String,
    pub source_pins: Vec<SourcePin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcePin {
    pub source_ref: String,
    pub watermark: String,
}

// ---------------------------------------------------------------------------
// LocalBuildRunner — MVP local process runner (M15 §6.3)
//
// MVP: local process execution with timeout enforcement, build log capture,
// and artifact hash recording. No container isolation yet.
// ---------------------------------------------------------------------------

pub struct LocalBuildRunner {
    config: BuildConfig,
}

impl LocalBuildRunner {
    pub fn new(config: BuildConfig) -> Self {
        Self { config }
    }

    /// Verify the sandbox invariants (network policy, timeout configured).
    pub fn verify_sandbox(&self) -> Vec<String> {
        let mut issues = Vec::new();
        if self.config.network_policy != crate::runtime::config::NetworkPolicy::DefaultDeny {
            issues.push("Build network policy is not DefaultDeny".to_string());
        }
        if self.config.timeout.is_zero() {
            issues.push("Build timeout is zero".to_string());
        }
        issues
    }
}

impl BuildRunner for LocalBuildRunner {
    /// Execute a build locally.
    ///
    /// In the MVP this is a simulated build that:
    /// 1. Checks timeout budget
    /// 2. Computes an artifact hash from the spec
    /// 3. Records a build log
    ///
    /// Real implementation would spawn a child process.
    fn run(&self, spec: &BuildSpec) -> BuildOutcome {
        let start = Instant::now();

        // Simulate build work — in production this would exec a process.
        let log = format!(
            "Build started for {}\nEntrypoint: {}\nSource pins: {}\nNetwork: {:?}\nTimeout: {:?}\nBuild completed.",
            spec.projection_id,
            spec.entrypoint,
            spec.source_pins.len(),
            self.config.network_policy,
            self.config.timeout,
        );

        // Deterministic artifact hash from spec content.
        let hash_input = format!(
            "{}:{}:{}",
            spec.projection_id,
            spec.entrypoint,
            spec.source_pins
                .iter()
                .map(|p| format!("{}@{}", p.source_ref, p.watermark))
                .collect::<Vec<_>>()
                .join(",")
        );
        let artifact_hash = format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::finalize(
                <sha2::Sha256 as sha2::Digest>::new_with_prefix(hash_input.as_bytes())
            )
        );

        let duration = start.elapsed();
        let timed_out = duration > self.config.timeout;

        BuildOutcome {
            success: !timed_out,
            artifact_hash: Some(artifact_hash),
            duration,
            log,
            timed_out,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::config::{BuildConfig, NetworkPolicy};

    fn default_runner() -> LocalBuildRunner {
        LocalBuildRunner::new(BuildConfig::default())
    }

    fn test_spec() -> BuildSpec {
        BuildSpec {
            projection_id: "proj:person-page".into(),
            entrypoint: "build.sql".into(),
            source_pins: vec![SourcePin {
                source_ref: "lake".into(),
                watermark: "wm:100".into(),
            }],
        }
    }

    #[test]
    fn build_succeeds_with_hash() {
        let runner = default_runner();
        let outcome = runner.run(&test_spec());
        assert!(outcome.success);
        assert!(!outcome.timed_out);
        assert!(outcome.artifact_hash.is_some());
        assert!(!outcome.artifact_hash.as_ref().unwrap().is_empty());
        assert!(outcome.log.contains("Build completed"));
    }

    #[test]
    fn artifact_hash_is_deterministic() {
        let runner = default_runner();
        let spec = test_spec();
        let h1 = runner.run(&spec).artifact_hash.unwrap();
        let h2 = runner.run(&spec).artifact_hash.unwrap();
        assert_eq!(h1, h2, "Same spec must produce same artifact hash");
    }

    #[test]
    fn different_spec_different_hash() {
        let runner = default_runner();
        let spec1 = test_spec();
        let spec2 = BuildSpec {
            projection_id: "proj:identity-resolution".into(),
            ..test_spec()
        };
        let h1 = runner.run(&spec1).artifact_hash.unwrap();
        let h2 = runner.run(&spec2).artifact_hash.unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn verify_sandbox_default_ok() {
        let runner = default_runner();
        assert!(runner.verify_sandbox().is_empty());
    }

    #[test]
    fn verify_sandbox_warns_unrestricted_network() {
        let runner = LocalBuildRunner::new(BuildConfig {
            network_policy: NetworkPolicy::Unrestricted,
            ..BuildConfig::default()
        });
        let issues = runner.verify_sandbox();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("DefaultDeny"));
    }

    #[test]
    fn build_log_contains_spec_info() {
        let runner = default_runner();
        let outcome = runner.run(&test_spec());
        assert!(outcome.log.contains("proj:person-page"));
        assert!(outcome.log.contains("build.sql"));
    }

    #[test]
    fn build_spec_round_trips_via_json() {
        let spec = test_spec();
        let json = serde_json::to_string(&spec).unwrap();
        let back: BuildSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.projection_id, spec.projection_id);
        assert_eq!(back.source_pins.len(), 1);
    }

    #[test]
    fn build_outcome_serializes() {
        let runner = default_runner();
        let outcome = runner.run(&test_spec());
        let json = serde_json::to_string(&outcome).unwrap();
        assert!(json.contains("artifact_hash"));
    }
}
