use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use lethe_core::domain::types::ProjectionHealth;
use lethe_core::domain::values::ProjectionRef;

// ---------------------------------------------------------------------------
// ServiceHealth — aggregated system health (M15 §8, M14 §8)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceHealth {
    pub status: HealthStatus,
    pub version: String,
    pub projections: HashMap<String, ProjectionHealthInfo>,
    pub components: HashMap<String, ComponentHealth>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ok,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionHealthInfo {
    pub status: ProjectionHealth,
    pub built_at: Option<String>,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
}

// ---------------------------------------------------------------------------
// HealthAggregator — build ServiceHealth from component statuses
// ---------------------------------------------------------------------------

pub struct HealthAggregator {
    version: String,
}

impl HealthAggregator {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
        }
    }

    /// Build overall health from individual component and projection statuses.
    pub fn aggregate(
        &self,
        components: Vec<ComponentHealth>,
        projections: Vec<(ProjectionRef, ProjectionHealthInfo)>,
    ) -> ServiceHealth {
        let proj_map: HashMap<String, ProjectionHealthInfo> = projections
            .into_iter()
            .map(|(r, info)| (r.as_str().to_string(), info))
            .collect();

        let comp_map: HashMap<String, ComponentHealth> = components
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();

        let overall = Self::compute_overall(&components, &proj_map);

        ServiceHealth {
            status: overall,
            version: self.version.clone(),
            projections: proj_map,
            components: comp_map,
        }
    }

    fn compute_overall(
        components: &[ComponentHealth],
        projections: &HashMap<String, ProjectionHealthInfo>,
    ) -> HealthStatus {
        let any_unhealthy = components
            .iter()
            .any(|c| c.status == HealthStatus::Unhealthy);
        if any_unhealthy {
            return HealthStatus::Unhealthy;
        }

        let any_degraded = components
            .iter()
            .any(|c| c.status == HealthStatus::Degraded)
            || projections.values().any(|p| {
                matches!(
                    p.status,
                    ProjectionHealth::Degraded | ProjectionHealth::Broken
                )
            });
        if any_degraded {
            return HealthStatus::Degraded;
        }

        HealthStatus::Ok
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn agg() -> HealthAggregator {
        HealthAggregator::new("0.1.0")
    }

    #[test]
    fn all_healthy() {
        let health = agg().aggregate(
            vec![ComponentHealth {
                name: "lake".into(),
                status: HealthStatus::Ok,
                message: None,
            }],
            vec![(
                ProjectionRef::new("proj:person-page"),
                ProjectionHealthInfo {
                    status: ProjectionHealth::Healthy,
                    built_at: Some("2026-01-01T00:00:00Z".into()),
                    stale: false,
                },
            )],
        );
        assert_eq!(health.status, HealthStatus::Ok);
        assert_eq!(health.version, "0.1.0");
    }

    #[test]
    fn degraded_on_stale_projection() {
        let health = agg().aggregate(
            vec![ComponentHealth {
                name: "lake".into(),
                status: HealthStatus::Ok,
                message: None,
            }],
            vec![(
                ProjectionRef::new("proj:person-page"),
                ProjectionHealthInfo {
                    status: ProjectionHealth::Degraded,
                    built_at: Some("2026-01-01T00:00:00Z".into()),
                    stale: true,
                },
            )],
        );
        assert_eq!(health.status, HealthStatus::Degraded);
    }

    #[test]
    fn unhealthy_on_component_down() {
        let health = agg().aggregate(
            vec![ComponentHealth {
                name: "registry".into(),
                status: HealthStatus::Unhealthy,
                message: Some("connection refused".into()),
            }],
            vec![],
        );
        assert_eq!(health.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn empty_components_is_ok() {
        let health = agg().aggregate(vec![], vec![]);
        assert_eq!(health.status, HealthStatus::Ok);
    }

    #[test]
    fn health_round_trips_via_json() {
        let health = agg().aggregate(
            vec![ComponentHealth {
                name: "lake".into(),
                status: HealthStatus::Ok,
                message: None,
            }],
            vec![],
        );
        let json = serde_json::to_string(&health).unwrap();
        let back: ServiceHealth = serde_json::from_str(&json).unwrap();
        assert_eq!(back.status, HealthStatus::Ok);
    }
}
