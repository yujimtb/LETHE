//! M02 Registry — EntityType definitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::EntityTypeRef;

/// A registered observation-target type (e.g. `et:person`, `et:room`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityType {
    /// `et:{name}` format.
    pub id: EntityTypeRef,
    pub name: String,
    pub description: String,
    /// Optional parent for is-a hierarchy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<EntityTypeRef>,
    /// Recommended attribute names on observations of this type.
    #[serde(default)]
    pub attributes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_at: Option<DateTime<Utc>>,
}

/// Foundation entity types that are always present.
pub fn base_entity_types() -> Vec<EntityType> {
    let now = Utc::now();
    serde_json::from_str::<Vec<SeedEntityType>>(include_str!("../../seeds/entity_types.json"))
        .expect("entity type seed data must be valid JSON")
        .into_iter()
        .map(|seed| EntityType {
            id: EntityTypeRef::new(seed.id),
            name: seed.name,
            description: seed.description,
            parent: seed.parent.map(EntityTypeRef::new),
            attributes: seed.attributes,
            registered_by: Some("system".into()),
            registered_at: Some(now),
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct SeedEntityType {
    id: String,
    name: String,
    description: String,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    attributes: Vec<String>,
}
