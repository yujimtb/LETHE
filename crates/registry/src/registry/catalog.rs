//! M02 Registry — Projection Catalog entry

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use lethe_core::domain::{
    ProjectionHealth, ProjectionKind, ProjectionRef, ProjectionStatus, SemVer,
};

/// An entry in the Projection Catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionCatalogEntry {
    /// `proj:{name}` format.
    pub id: ProjectionRef,
    pub name: String,
    pub description: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub version: SemVer,
    pub status: ProjectionStatus,
    pub kind: ProjectionKind,
    pub engine: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub health: ProjectionHealth,
    /// Auto-calculated DAG depth (0 = no projection dependencies).
    pub depth: u32,
}
