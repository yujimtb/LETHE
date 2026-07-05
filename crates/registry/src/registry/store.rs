//! M02 Registry — In-memory store with invariant enforcement
//!
//! MVP implementation.  Can be swapped to SQLite / PostgreSQL later without
//! changing the domain rules enforced here.

use std::collections::HashMap;

use lethe_core::domain::{
    DomainError, EntityTypeRef, ObserverRef, ProjectionRef, SchemaRef, SemVer, SourceSystemRef,
};

use super::supplemental_kind::{
    SupplementalKindError, SupplementalKindSchema, SupplementalKindValidationConfig,
    SupplementalKindVersion, parse_semver, parse_supplemental_kind_ref, supplemental_kind_key,
    supplemental_kind_key_for_schema, validate_json_schema_document, validate_supplemental_payload,
    validate_supplemental_record_claim_anchor,
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
    supplemental_kind_schemas: HashMap<String, SupplementalKindSchema>,
    supplemental_kind_versions: Vec<SupplementalKindVersion>,
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
            payload_schema: payload_schema.clone(),
            created_at: chrono::Utc::now(),
        };
        self.schema_versions.push(ver);
        // Update the "latest" pointer.
        if let Some(s) = self.schemas.get_mut(&id.0) {
            s.version = version;
            s.payload_schema = payload_schema;
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
    // Supplemental Kind Schema
    // -----------------------------------------------------------------------

    pub fn register_supplemental_kind_schema(
        &mut self,
        schema: SupplementalKindSchema,
    ) -> Result<(), SupplementalKindError> {
        validate_json_schema_document(&schema)?;
        let key = supplemental_kind_key_for_schema(&schema)?;

        if let Some(current) = self.latest_supplemental_kind_schema_for_kind(&schema.kind) {
            validate_schema_version_transition(
                &current.version,
                &current.payload_schema,
                &schema.version,
                &schema.payload_schema,
            )
            .map_err(
                |message| SupplementalKindError::SchemaVersionRuleViolation {
                    kind: schema.kind.clone(),
                    current_version: current.version.clone(),
                    next_version: schema.version.clone(),
                    message,
                },
            )?;
        }

        let version = SupplementalKindVersion {
            kind: schema.kind.clone(),
            version: schema.version.clone(),
            payload_schema: schema.payload_schema.clone(),
            created_at: chrono::Utc::now(),
        };
        self.supplemental_kind_schemas.insert(key, schema);
        self.supplemental_kind_versions.push(version);
        Ok(())
    }

    pub fn get_supplemental_kind_schema(
        &self,
        kind: &str,
        major_version: u64,
    ) -> Option<&SupplementalKindSchema> {
        self.supplemental_kind_schemas
            .get(&supplemental_kind_key(kind, major_version))
    }

    pub fn get_supplemental_kind_versions(&self, kind: &str) -> Vec<&SupplementalKindVersion> {
        self.supplemental_kind_versions
            .iter()
            .filter(|version| version.kind == kind)
            .collect()
    }

    pub fn list_supplemental_kind_schemas(&self) -> Vec<&SupplementalKindSchema> {
        self.supplemental_kind_schemas.values().collect()
    }

    fn latest_supplemental_kind_schema_for_kind(
        &self,
        kind: &str,
    ) -> Option<&SupplementalKindSchema> {
        self.supplemental_kind_schemas
            .values()
            .filter(|schema| schema.kind == kind)
            .max_by_key(|schema| parse_semver(schema.version.as_str()))
    }

    pub fn validate_supplemental_payload_for_kind(
        &self,
        config: SupplementalKindValidationConfig,
        kind: &str,
        major_version: u64,
        payload: &serde_json::Value,
    ) -> Result<(), SupplementalKindError> {
        let schema = self
            .get_supplemental_kind_schema(kind, major_version)
            .ok_or_else(|| {
                if config.reject_unregistered_kinds {
                    SupplementalKindError::KindNotRegistered {
                        kind: kind.to_owned(),
                        major_version,
                    }
                } else {
                    SupplementalKindError::UnregisteredKindPolicyDisabled {
                        kind: kind.to_owned(),
                        major_version,
                    }
                }
            })?;
        validate_supplemental_payload(schema, payload)
    }

    pub fn validate_supplemental_record_kind<F>(
        &self,
        config: SupplementalKindValidationConfig,
        record: &lethe_core::domain::SupplementalRecord,
        mut supplemental_kind_for_id: F,
    ) -> Result<(), SupplementalKindError>
    where
        F: FnMut(&lethe_core::domain::SupplementalId) -> Option<String>,
    {
        let kind_ref = parse_supplemental_kind_ref(&record.kind)?;
        self.validate_supplemental_payload_for_kind(
            config,
            &kind_ref.kind,
            kind_ref.major_version,
            &record.payload,
        )?;
        validate_supplemental_record_claim_anchor(record, |id| supplemental_kind_for_id(id))
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
        status: lethe_core::domain::ProjectionStatus,
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
    validate_schema_version_transition(current_version, current_schema, next_version, next_schema)
        .map_err(DomainError::Validation)
}

fn validate_schema_version_transition(
    current_version: &SemVer,
    current_schema: &serde_json::Value,
    next_version: &SemVer,
    next_schema: &serde_json::Value,
) -> Result<(), String> {
    let current = parse_semver(current_version.as_str())
        .ok_or_else(|| format!("current schema version {current_version} is not SemVer"))?;
    let next = parse_semver(next_version.as_str())
        .ok_or_else(|| format!("next schema version {next_version} is not SemVer"))?;

    if next <= current {
        return Err(format!(
            "next schema version {next_version} must be greater than current version {current_version}"
        ));
    }
    if next.major > current.major {
        return Ok(());
    }
    if next.major < current.major {
        return Err(format!(
            "next schema version {next_version} must not go backwards from {current_version}"
        ));
    }

    let current_required = required_fields(current_schema);
    let next_required = required_fields(next_schema);
    let added_required = next_required
        .difference(&current_required)
        .cloned()
        .collect::<Vec<_>>();
    let removed_required = current_required
        .difference(&next_required)
        .cloned()
        .collect::<Vec<_>>();
    if !added_required.is_empty() || !removed_required.is_empty() {
        return Err(format!(
            "required field changes require a major version bump; added={added_required:?}, removed={removed_required:?}"
        ));
    }

    let current_properties = object_properties(current_schema);
    let next_properties = object_properties(next_schema);
    let removed_properties = current_properties
        .keys()
        .filter(|field| !next_properties.contains_key(*field))
        .cloned()
        .collect::<Vec<_>>();
    if !removed_properties.is_empty() {
        return Err(format!(
            "field deletion requires a major version bump; removed={removed_properties:?}"
        ));
    }

    let changed_properties = current_properties
        .iter()
        .filter_map(|(field, current)| {
            next_properties
                .get(field)
                .filter(|next| *next != current)
                .map(|_| field.clone())
        })
        .collect::<Vec<_>>();
    if !changed_properties.is_empty() {
        return Err(format!(
            "field schema changes require a major version bump; changed={changed_properties:?}"
        ));
    }

    let added_properties = next_properties
        .keys()
        .filter(|field| !current_properties.contains_key(*field))
        .cloned()
        .collect::<Vec<_>>();
    if !added_properties.is_empty() && next.minor == current.minor {
        return Err(format!(
            "optional field additions require a minor version bump; added={added_properties:?}"
        ));
    }

    Ok(())
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

fn object_properties(
    schema: &serde_json::Value,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    schema
        .get("properties")
        .and_then(|value| value.as_object())
        .into_iter()
        .flatten()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::*;
    use lethe_core::domain::*;

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

    // -- Supplemental Kind Schema ------------------------------------------

    fn store_with_base_supplemental_kinds() -> RegistryStore {
        let mut store = RegistryStore::new();
        for schema in base_supplemental_kind_schemas() {
            store.register_supplemental_kind_schema(schema).unwrap();
        }
        store
    }

    #[test]
    fn supplemental_kind_register_and_get_by_kind_and_major_version() {
        let store = store_with_base_supplemental_kinds();

        let schema = store.get_supplemental_kind_schema("claim", 1).unwrap();
        assert_eq!(schema.kind, "claim");
        assert_eq!(schema.version, SemVer::new("1.0.0"));
    }

    #[test]
    fn supplemental_kind_same_major_rejects_required_addition() {
        let mut store = RegistryStore::new();
        store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("1.0.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement"],
                    "properties": {
                        "statement": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap();

        let err = store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("1.1.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement", "verification_mode"],
                    "properties": {
                        "statement": { "type": "string" },
                        "verification_mode": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap_err();

        assert!(matches!(
            err,
            SupplementalKindError::SchemaVersionRuleViolation { .. }
        ));
    }

    #[test]
    fn supplemental_kind_same_major_rejects_required_removal() {
        let mut store = RegistryStore::new();
        store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("1.0.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement", "verification_mode"],
                    "properties": {
                        "statement": { "type": "string" },
                        "verification_mode": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap();

        let err = store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("1.1.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement"],
                    "properties": {
                        "statement": { "type": "string" },
                        "verification_mode": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap_err();

        assert!(matches!(
            err,
            SupplementalKindError::SchemaVersionRuleViolation { .. }
        ));
    }

    #[test]
    fn supplemental_kind_minor_allows_optional_field_addition() {
        let mut store = RegistryStore::new();
        store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("1.0.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement"],
                    "properties": {
                        "statement": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap();

        store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("1.1.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement"],
                    "properties": {
                        "statement": { "type": "string" },
                        "context": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap();

        assert_eq!(
            store
                .get_supplemental_kind_schema("claim", 1)
                .unwrap()
                .version,
            SemVer::new("1.1.0")
        );
    }

    #[test]
    fn supplemental_kind_rejects_version_order_regression_across_major_keys() {
        let mut store = RegistryStore::new();
        store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("2.0.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement"],
                    "properties": {
                        "statement": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap();

        let err = store
            .register_supplemental_kind_schema(SupplementalKindSchema {
                kind: "claim".into(),
                version: SemVer::new("1.1.0"),
                payload_schema: serde_json::json!({
                    "type": "object",
                    "required": ["statement"],
                    "properties": {
                        "statement": { "type": "string" }
                    }
                }),
                registered_by: None,
                registered_at: None,
            })
            .unwrap_err();

        assert!(matches!(
            err,
            SupplementalKindError::SchemaVersionRuleViolation { .. }
        ));
    }

    #[test]
    fn supplemental_payload_detects_required_type_and_enum_violations() {
        let store = store_with_base_supplemental_kinds();
        let err = store
            .validate_supplemental_payload_for_kind(
                SupplementalKindValidationConfig {
                    reject_unregistered_kinds: true,
                },
                "claim",
                1,
                &serde_json::json!({
                    "statement": 42,
                    "verification_mode": "later"
                }),
            )
            .unwrap_err();

        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
            panic!("expected payload schema violation");
        };
        assert!(violations.iter().any(|v| v.field == "statement"));
        assert!(violations.iter().any(|v| v.field == "verification_mode"));

        let missing = store
            .validate_supplemental_payload_for_kind(
                SupplementalKindValidationConfig {
                    reject_unregistered_kinds: true,
                },
                "claim",
                1,
                &serde_json::json!({
                    "statement": "検証対象"
                }),
            )
            .unwrap_err();
        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = missing else {
            panic!("expected payload schema violation");
        };
        assert!(violations.iter().any(|v| v.field == "verification_mode"));
    }

    #[test]
    fn parking_without_resume_context_is_rejected() {
        let store = store_with_base_supplemental_kinds();
        let err = store
            .validate_supplemental_payload_for_kind(
                SupplementalKindValidationConfig {
                    reject_unregistered_kinds: true,
                },
                "parking",
                1,
                &serde_json::json!({
                    "statement": "ここで中断する"
                }),
            )
            .unwrap_err();

        let SupplementalKindError::PayloadSchemaViolation { violations, .. } = err else {
            panic!("expected payload schema violation");
        };
        assert!(violations.iter().any(|v| v.field == "resume_context"));
    }

    #[test]
    fn unregistered_supplemental_kind_is_rejected() {
        let store = store_with_base_supplemental_kinds();
        let err = store
            .validate_supplemental_payload_for_kind(
                SupplementalKindValidationConfig {
                    reject_unregistered_kinds: true,
                },
                "random-note",
                1,
                &serde_json::json!({ "statement": "unknown" }),
            )
            .unwrap_err();

        assert!(matches!(
            err,
            SupplementalKindError::KindNotRegistered {
                kind,
                major_version: 1
            } if kind == "random-note"
        ));
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
