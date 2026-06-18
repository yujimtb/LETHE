//! M14: Health Endpoint — projection health summary.

use serde::{Deserialize, Serialize};

use crate::domain::{ProjectionHealth, ProjectionStatus};
use crate::projection::catalog::ProjectionCatalog;

/// Per-projection health info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionHealthInfo {
    pub id: String,
    pub status: ProjectionStatus,
    pub health: ProjectionHealth,
}

/// Health endpoint response (M14 §8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub projections: Vec<ProjectionHealthInfo>,
}

impl HealthResponse {
    /// Build health response from catalog state.
    pub fn from_catalog(catalog: &ProjectionCatalog, app_version: &str) -> Self {
        let projections: Vec<ProjectionHealthInfo> = catalog
            .entries()
            .map(|entry| ProjectionHealthInfo {
                id: entry.spec.id.as_str().to_string(),
                status: entry.status,
                health: entry.health,
            })
            .collect();

        let all_healthy = projections
            .iter()
            .all(|p| p.health == ProjectionHealth::Healthy);
        let status = if all_healthy { "ok" } else { "degraded" };

        Self {
            status: status.into(),
            version: app_version.into(),
            projections,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    use crate::projection::catalog::ProjectionCatalog;
    use crate::projection::spec::*;

    fn lake_spec(id: &str) -> ProjectionSpec {
        ProjectionSpec {
            id: ProjectionRef::new(id),
            name: id.into(),
            version: SemVer::new("1.0.0"),
            kind: ProjectionKind::PureProjection,
            sources: vec![SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![],
                filter_derivations: vec![],
            }],
            read_modes: vec![ReadModePolicy {
                mode: ReadMode::OperationalLatest,
                source_policy: "lake-latest".into(),
            }],
            build: BuildSpec {
                build_type: "rust".into(),
                entrypoint: None,
                projector: "p".into(),
            },
            outputs: vec![OutputSpec {
                format: "sql".into(),
                tables: vec!["t".into()],
            }],
            reconciliation: None,
            deterministic_in: vec![],
            gap_action: None,
            tags: vec![],
            description: None,
            created_by: "test".into(),
        }
    }

    #[test]
    fn health_ok_when_all_healthy() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        catalog.set_health(&ProjectionRef::new("proj:a"), ProjectionHealth::Healthy);

        let resp = HealthResponse::from_catalog(&catalog, "0.1.0");
        assert_eq!(resp.status, "ok");
    }

    #[test]
    fn health_degraded_when_broken() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        catalog.set_health(&ProjectionRef::new("proj:a"), ProjectionHealth::Broken);

        let resp = HealthResponse::from_catalog(&catalog, "0.1.0");
        assert_eq!(resp.status, "degraded");
    }
}
