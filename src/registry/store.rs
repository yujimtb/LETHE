//! M02 Registry — In-memory store with invariant enforcement
//!
//! MVP implementation.  Can be swapped to SQLite / PostgreSQL later without
//! changing the domain rules enforced here.

use std::collections::HashMap;

use crate::domain::{
    DomainError, EntityTypeRef, ObserverRef, ProjectionRef, SchemaRef, SemVer, SourceSystemRef,
};

use super::{
    EntityType, ObservationSchema, Observer, ProjectionCatalogEntry, SchemaVersion, SourceSystem,
};

/// In-memory registry that enforces all M02 invariants.
#[derive(Debug, Default)]
pub struct RegistryStore {
    entity_types: HashMap<String, EntityType>,
    schemas: HashMap<String, ObservationSchema>,
    schema_versions: Vec<SchemaVersion>,
    observers: HashMap<String, Observer>,
    source_systems: HashMap<String, SourceSystem>,
    projections: HashMap<String, ProjectionCatalogEntry>,
}

impl RegistryStore {
    pub fn new() -> Self {
        let mut store = Self::default();
        // Seed base entity types.
        for et in super::base_entity_types() {
            store.entity_types.insert(et.id.0.clone(), et);
        }
        store
    }

    // -----------------------------------------------------------------------
    // EntityType
    // -----------------------------------------------------------------------

    pub fn register_entity_type(&mut self, et: EntityType) -> Result<(), DomainError> {
        if self.entity_types.contains_key(&et.id.0) {
            return Err(DomainError::Conflict(format!(
                "EntityType {} already exists",
                et.id
            )));
        }
        // Validate parent exists if specified.
        if let Some(ref parent) = et.parent
            && !self.entity_types.contains_key(&parent.0)
        {
            return Err(DomainError::Validation(format!(
                "Parent EntityType {} does not exist",
                parent
            )));
        }
        self.entity_types.insert(et.id.0.clone(), et);
        Ok(())
    }

    pub fn get_entity_type(&self, id: &EntityTypeRef) -> Option<&EntityType> {
        self.entity_types.get(&id.0)
    }

    pub fn list_entity_types(&self) -> Vec<&EntityType> {
        self.entity_types.values().collect()
    }

    // -----------------------------------------------------------------------
    // Schema
    // -----------------------------------------------------------------------

    pub fn register_schema(&mut self, schema: ObservationSchema) -> Result<(), DomainError> {
        if self.schemas.contains_key(&schema.id.0) {
            return Err(DomainError::Conflict(format!(
                "Schema {} already exists – use add_schema_version for new versions",
                schema.id
            )));
        }
        // Subject type must exist (or be wildcard "et:*").
        if schema.subject_type.0 != "et:*"
            && !self.entity_types.contains_key(&schema.subject_type.0)
        {
            return Err(DomainError::Validation(format!(
                "Subject EntityType {} does not exist",
                schema.subject_type
            )));
        }
        let ver = SchemaVersion {
            schema_id: schema.id.clone(),
            version: schema.version.clone(),
            payload_schema: schema.payload_schema.clone(),
            created_at: chrono::Utc::now(),
        };
        self.schemas.insert(schema.id.0.clone(), schema);
        self.schema_versions.push(ver);
        Ok(())
    }

    pub fn add_schema_version(
        &mut self,
        id: &SchemaRef,
        version: SemVer,
        payload_schema: serde_json::Value,
    ) -> Result<(), DomainError> {
        let current = self
            .schemas
            .get(&id.0)
            .ok_or_else(|| DomainError::NotFound(format!("Schema {} not found", id)))?;
        validate_schema_compatibility(
            &current.version,
            &current.payload_schema,
            &version,
            &payload_schema,
        )?;
        if !self.schemas.contains_key(&id.0) {
            return Err(DomainError::NotFound(format!("Schema {} not found", id)));
        }
        let ver = SchemaVersion {
            schema_id: id.clone(),
            version: version.clone(),
            payload_schema,
            created_at: chrono::Utc::now(),
        };
        self.schema_versions.push(ver);
        // Update the "latest" pointer.
        if let Some(s) = self.schemas.get_mut(&id.0) {
            s.version = version;
        }
        Ok(())
    }

    pub fn get_schema(&self, id: &SchemaRef) -> Option<&ObservationSchema> {
        self.schemas.get(&id.0)
    }

    pub fn get_schema_versions(&self, id: &SchemaRef) -> Vec<&SchemaVersion> {
        self.schema_versions
            .iter()
            .filter(|v| v.schema_id == *id)
            .collect()
    }

    pub fn list_schemas(&self) -> Vec<&ObservationSchema> {
        self.schemas.values().collect()
    }

    // -----------------------------------------------------------------------
    // SourceSystem
    // -----------------------------------------------------------------------

    pub fn register_source_system(&mut self, ss: SourceSystem) -> Result<(), DomainError> {
        if self.source_systems.contains_key(&ss.id.0) {
            return Err(DomainError::Conflict(format!(
                "SourceSystem {} already exists",
                ss.id
            )));
        }
        self.source_systems.insert(ss.id.0.clone(), ss);
        Ok(())
    }

    pub fn get_source_system(&self, id: &SourceSystemRef) -> Option<&SourceSystem> {
        self.source_systems.get(&id.0)
    }

    pub fn list_source_systems(&self) -> Vec<&SourceSystem> {
        self.source_systems.values().collect()
    }

    // -----------------------------------------------------------------------
    // Observer
    // -----------------------------------------------------------------------

    pub fn register_observer(&mut self, obs: Observer) -> Result<(), DomainError> {
        if self.observers.contains_key(&obs.id.0) {
            return Err(DomainError::Conflict(format!(
                "Observer {} already exists",
                obs.id
            )));
        }
        // Source system must exist.
        if !self.source_systems.contains_key(&obs.source_system.0) {
            return Err(DomainError::Validation(format!(
                "SourceSystem {} does not exist",
                obs.source_system
            )));
        }
        self.observers.insert(obs.id.0.clone(), obs);
        Ok(())
    }

    pub fn get_observer(&self, id: &ObserverRef) -> Option<&Observer> {
        self.observers.get(&id.0)
    }

    pub fn list_observers(&self) -> Vec<&Observer> {
        self.observers.values().collect()
    }

    // -----------------------------------------------------------------------
    // Projection Catalog
    // -----------------------------------------------------------------------

    pub fn register_projection(
        &mut self,
        entry: ProjectionCatalogEntry,
    ) -> Result<(), DomainError> {
        if self.projections.contains_key(&entry.id.0) {
            return Err(DomainError::Conflict(format!(
                "Projection {} already exists",
                entry.id
            )));
        }
        self.projections.insert(entry.id.0.clone(), entry);
        Ok(())
    }

    pub fn get_projection(&self, id: &ProjectionRef) -> Option<&ProjectionCatalogEntry> {
        self.projections.get(&id.0)
    }

    pub fn list_projections(&self) -> Vec<&ProjectionCatalogEntry> {
        self.projections.values().collect()
    }

    pub fn update_projection_status(
        &mut self,
        id: &ProjectionRef,
        status: crate::domain::ProjectionStatus,
    ) -> Result<(), DomainError> {
        let entry = self
            .projections
            .get_mut(&id.0)
            .ok_or_else(|| DomainError::NotFound(format!("Projection {} not found", id)))?;
        entry.status = status;
        Ok(())
    }
}

fn validate_schema_compatibility(
    current_version: &SemVer,
    current_schema: &serde_json::Value,
    next_version: &SemVer,
    next_schema: &serde_json::Value,
) -> Result<(), DomainError> {
    let Some((current_major, _, _)) = parse_semver(current_version.as_str()) else {
        return Err(DomainError::Validation(format!(
            "Current schema version {} is not SemVer",
            current_version
        )));
    };
    let Some((next_major, _, _)) = parse_semver(next_version.as_str()) else {
        return Err(DomainError::Validation(format!(
            "Next schema version {} is not SemVer",
            next_version
        )));
    };
    if next_major > current_major {
        return Ok(());
    }

    let current_required = required_fields(current_schema);
    let next_required = required_fields(next_schema);
    let added = next_required
        .difference(&current_required)
        .cloned()
        .collect::<Vec<_>>();
    if added.is_empty() {
        Ok(())
    } else {
        Err(DomainError::Validation(format!(
            "Schema {} adds required fields {:?} without a major version bump",
            next_version, added
        )))
    }
}

fn parse_semver(raw: &str) -> Option<(u64, u64, u64)> {
    let mut parts = raw.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

fn required_fields(schema: &serde_json::Value) -> std::collections::BTreeSet<String> {
    schema
        .get("required")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
        .collect()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    use crate::registry::*;

    fn make_store_with_source() -> RegistryStore {
        let mut store = RegistryStore::new();
        store
            .register_source_system(SourceSystem {
                id: SourceSystemRef::new("sys:slack"),
                name: "Slack".into(),
                provider: Some("Slack".into()),
                api_version: Some("v1".into()),
                source_class: SourceClass::MutableText,
            })
            .unwrap();
        store
    }

    // -- EntityType ---------------------------------------------------------

    #[test]
    fn base_types_are_seeded() {
        let store = RegistryStore::new();
        assert!(
            store
                .get_entity_type(&EntityTypeRef::new("et:person"))
                .is_some()
        );
        assert!(
            store
                .get_entity_type(&EntityTypeRef::new("et:document"))
                .is_some()
        );
    }

    #[test]
    fn duplicate_entity_type_rejected() {
        let mut store = RegistryStore::new();
        let et = EntityType {
            id: EntityTypeRef::new("et:person"),
            name: "Person".into(),
            description: "dup".into(),
            parent: None,
            attributes: vec![],
            registered_by: None,
            registered_at: None,
        };
        assert!(store.register_entity_type(et).is_err());
    }

    #[test]
    fn entity_type_with_missing_parent_rejected() {
        let mut store = RegistryStore::new();
        let et = EntityType {
            id: EntityTypeRef::new("et:special-room"),
            name: "Special Room".into(),
            description: "test".into(),
            parent: Some(EntityTypeRef::new("et:nonexistent")),
            attributes: vec![],
            registered_by: None,
            registered_at: None,
        };
        assert!(store.register_entity_type(et).is_err());
    }

    #[test]
    fn entity_type_with_valid_parent_accepted() {
        let mut store = RegistryStore::new();
        let et = EntityType {
            id: EntityTypeRef::new("et:room"),
            name: "Room".into(),
            description: "a room".into(),
            parent: Some(EntityTypeRef::new("et:space")),
            attributes: vec![],
            registered_by: None,
            registered_at: None,
        };
        assert!(store.register_entity_type(et).is_ok());
    }

    // -- Schema -------------------------------------------------------------

    #[test]
    fn schema_register_and_version() {
        let mut store = RegistryStore::new();
        let schema = ObservationSchema {
            id: SchemaRef::new("schema:slack-message"),
            name: "Slack Message".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:message"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        };
        store.register_schema(schema).unwrap();

        // Add a minor version.
        store
            .add_schema_version(
                &SchemaRef::new("schema:slack-message"),
                SemVer::new("1.1.0"),
                serde_json::json!({"type": "object", "properties": {}}),
            )
            .unwrap();

        let versions = store.get_schema_versions(&SchemaRef::new("schema:slack-message"));
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn schema_minor_version_rejects_new_required_field() {
        let mut store = RegistryStore::new();
        let schema = ObservationSchema {
            id: SchemaRef::new("schema:room-entry"),
            name: "Room Entry".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:message"),
            target_type: None,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["id"]
            }),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        };
        store.register_schema(schema).unwrap();

        let err = store
            .add_schema_version(
                &SchemaRef::new("schema:room-entry"),
                SemVer::new("1.1.0"),
                serde_json::json!({
                    "type": "object",
                    "required": ["id", "room"]
                }),
            )
            .unwrap_err();
        assert!(matches!(err, DomainError::Validation(_)));
    }

    // -- Observer -----------------------------------------------------------

    #[test]
    fn observer_requires_source_system() {
        let mut store = RegistryStore::new();
        let obs = Observer {
            id: ObserverRef::new("obs:test"),
            name: "Test".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:nonexistent"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "test".into(),
            trust_level: TrustLevel::Automated,
        };
        assert!(store.register_observer(obs).is_err());
    }

    #[test]
    fn observer_with_source_system_accepted() {
        let mut store = make_store_with_source();
        let obs = Observer {
            id: ObserverRef::new("obs:slack-crawler"),
            name: "Slack Crawler".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:slack"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:slack-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        };
        assert!(store.register_observer(obs).is_ok());
    }
}
