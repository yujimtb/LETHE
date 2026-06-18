//! M05: Projection Catalog — registration, DAG acyclicity, status tracking.

use std::collections::{HashMap, HashSet};

use crate::domain::{ProjectionHealth, ProjectionRef, ProjectionStatus};

use super::spec::ProjectionSpec;

/// Entry in the catalog — a registered projection with its status.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub spec: ProjectionSpec,
    pub status: ProjectionStatus,
    pub health: ProjectionHealth,
}

/// Projection Catalog: manages registration, deduplication, and DAG validation.
#[derive(Debug, Default)]
pub struct ProjectionCatalog {
    entries: HashMap<String, CatalogEntry>,
}

impl ProjectionCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a projection spec. Rejects if DAG would become cyclic.
    pub fn register(&mut self, spec: ProjectionSpec) -> Result<(), CatalogError> {
        // Validate the spec first.
        spec.validate().map_err(|errs| {
            CatalogError::InvalidSpec(
                errs.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?;

        // Check for duplicate id.
        if self.entries.contains_key(spec.id.as_str()) {
            return Err(CatalogError::AlreadyRegistered(spec.id.clone()));
        }

        // Check DAG acyclicity: would adding this spec create a cycle?
        let deps: Vec<ProjectionRef> = spec
            .sources
            .iter()
            .filter_map(|s| s.projection_dep().cloned())
            .collect();

        for dep in &deps {
            if !self.entries.contains_key(dep.as_str()) {
                return Err(CatalogError::MissingDependency(dep.clone()));
            }
        }

        // Build adjacency and check for cycles.
        if self.would_create_cycle(&spec.id, &deps) {
            return Err(CatalogError::CyclicDependency);
        }

        self.entries.insert(
            spec.id.as_str().to_string(),
            CatalogEntry {
                spec,
                status: ProjectionStatus::Building,
                health: ProjectionHealth::Healthy,
            },
        );
        Ok(())
    }

    pub fn get(&self, id: &ProjectionRef) -> Option<&CatalogEntry> {
        self.entries.get(id.as_str())
    }

    pub fn get_mut(&mut self, id: &ProjectionRef) -> Option<&mut CatalogEntry> {
        self.entries.get_mut(id.as_str())
    }

    pub fn set_status(&mut self, id: &ProjectionRef, status: ProjectionStatus) -> bool {
        if let Some(entry) = self.entries.get_mut(id.as_str()) {
            entry.status = status;
            true
        } else {
            false
        }
    }

    pub fn set_health(&mut self, id: &ProjectionRef, health: ProjectionHealth) -> bool {
        if let Some(entry) = self.entries.get_mut(id.as_str()) {
            entry.health = health;
            true
        } else {
            false
        }
    }

    /// Return all registered projection ids.
    pub fn list_ids(&self) -> Vec<ProjectionRef> {
        self.entries.keys().map(|k| ProjectionRef::new(k)).collect()
    }

    /// Get all entries.
    pub fn entries(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries.values()
    }

    /// Topological sort of projection DAG.
    pub fn topological_order(&self) -> Result<Vec<ProjectionRef>, CatalogError> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();

        for (id, entry) in &self.entries {
            in_degree.entry(id.as_str()).or_insert(0);
            adj.entry(id.as_str()).or_default();
            for dep in entry.spec.sources.iter().filter_map(|s| s.projection_dep()) {
                adj.entry(dep.as_str()).or_default().push(id.as_str());
                *in_degree.entry(id.as_str()).or_insert(0) += 1;
            }
        }

        let mut queue: Vec<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(id, _)| *id)
            .collect();
        queue.sort(); // deterministic order

        let mut result = Vec::new();
        while let Some(node) = queue.pop() {
            result.push(ProjectionRef::new(node));
            if let Some(neighbors) = adj.get(node) {
                for &neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(neighbor);
                            queue.sort();
                        }
                    }
                }
            }
        }

        if result.len() != self.entries.len() {
            return Err(CatalogError::CyclicDependency);
        }
        Ok(result)
    }

    /// Downstream projections that depend on the given projection.
    pub fn dependents(&self, id: &ProjectionRef) -> Vec<ProjectionRef> {
        self.entries
            .values()
            .filter(|entry| {
                entry
                    .spec
                    .sources
                    .iter()
                    .any(|s| s.projection_dep().is_some_and(|dep| dep == id))
            })
            .map(|entry| entry.spec.id.clone())
            .collect()
    }

    // ---- internal ----------------------------------------------------------

    fn would_create_cycle(&self, new_id: &ProjectionRef, deps: &[ProjectionRef]) -> bool {
        // BFS from each dependency — if any reaches new_id, it's a cycle.
        for dep in deps {
            let mut visited = HashSet::new();
            let mut stack = vec![dep.as_str().to_string()];
            while let Some(current) = stack.pop() {
                if current == new_id.as_str() {
                    return true;
                }
                if !visited.insert(current.clone()) {
                    continue;
                }
                if let Some(entry) = self.entries.get(&current) {
                    for s in &entry.spec.sources {
                        if let Some(upstream) = s.projection_dep() {
                            stack.push(upstream.as_str().to_string());
                        }
                    }
                }
            }
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    InvalidSpec(String),
    AlreadyRegistered(ProjectionRef),
    MissingDependency(ProjectionRef),
    CyclicDependency,
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSpec(msg) => write!(f, "Invalid spec: {msg}"),
            Self::AlreadyRegistered(id) => write!(f, "Already registered: {id}"),
            Self::MissingDependency(id) => write!(f, "Missing dependency: {id}"),
            Self::CyclicDependency => write!(f, "Cyclic dependency detected"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    use crate::projection::spec::*;

    fn lake_spec(id: &str) -> ProjectionSpec {
        ProjectionSpec {
            id: ProjectionRef::new(id),
            name: id.into(),
            version: SemVer::new("1.0.0"),
            kind: ProjectionKind::PureProjection,
            sources: vec![SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![SchemaRef::new("schema:test")],
                filter_derivations: vec![],
            }],
            read_modes: vec![ReadModePolicy {
                mode: ReadMode::OperationalLatest,
                source_policy: "lake-latest".into(),
            }],
            build: BuildSpec {
                build_type: "rust".into(),
                entrypoint: None,
                projector: "proj".into(),
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

    fn dependent_spec(id: &str, deps: &[&str]) -> ProjectionSpec {
        let mut spec = lake_spec(id);
        spec.sources = deps
            .iter()
            .map(|dep| SourceDecl {
                source: SourceRef::Projection {
                    id: ProjectionRef::new(*dep),
                    version: ">=1.0.0".into(),
                },
                filter_schemas: vec![],
                filter_derivations: vec![],
            })
            .collect();
        // Also keep a Lake source so it's valid.
        spec.sources.push(SourceDecl {
            source: SourceRef::Lake,
            filter_schemas: vec![],
            filter_derivations: vec![],
        });
        spec.reconciliation = Some(ReconciliationPolicy::LakeFirst);
        spec
    }

    #[test]
    fn register_valid_projection() {
        let mut catalog = ProjectionCatalog::new();
        assert!(catalog.register(lake_spec("proj:a")).is_ok());
        assert!(catalog.get(&ProjectionRef::new("proj:a")).is_some());
    }

    #[test]
    fn duplicate_registration_rejected() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        let result = catalog.register(lake_spec("proj:a"));
        assert_eq!(
            result,
            Err(CatalogError::AlreadyRegistered(ProjectionRef::new(
                "proj:a"
            )))
        );
    }

    #[test]
    fn missing_dependency_rejected() {
        let mut catalog = ProjectionCatalog::new();
        let spec = dependent_spec("proj:b", &["proj:nonexistent"]);
        assert_eq!(
            catalog.register(spec),
            Err(CatalogError::MissingDependency(ProjectionRef::new(
                "proj:nonexistent"
            )))
        );
    }

    #[test]
    fn valid_dependency_chain() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        catalog
            .register(dependent_spec("proj:b", &["proj:a"]))
            .unwrap();
        assert!(catalog.get(&ProjectionRef::new("proj:b")).is_some());
    }

    #[test]
    fn topological_order_linear() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        catalog
            .register(dependent_spec("proj:b", &["proj:a"]))
            .unwrap();
        catalog
            .register(dependent_spec("proj:c", &["proj:b"]))
            .unwrap();

        let order = catalog.topological_order().unwrap();
        let ids: Vec<&str> = order.iter().map(|r| r.as_str()).collect();
        let pos_a = ids.iter().position(|&id| id == "proj:a").unwrap();
        let pos_b = ids.iter().position(|&id| id == "proj:b").unwrap();
        let pos_c = ids.iter().position(|&id| id == "proj:c").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn dependents_found() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        catalog
            .register(dependent_spec("proj:b", &["proj:a"]))
            .unwrap();
        catalog
            .register(dependent_spec("proj:c", &["proj:a"]))
            .unwrap();

        let deps = catalog.dependents(&ProjectionRef::new("proj:a"));
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn set_status_and_health() {
        let mut catalog = ProjectionCatalog::new();
        catalog.register(lake_spec("proj:a")).unwrap();
        let id = ProjectionRef::new("proj:a");
        assert!(catalog.set_status(&id, ProjectionStatus::Active));
        assert!(catalog.set_health(&id, ProjectionHealth::Stale));
        let entry = catalog.get(&id).unwrap();
        assert_eq!(entry.status, ProjectionStatus::Active);
        assert_eq!(entry.health, ProjectionHealth::Stale);
    }
}
