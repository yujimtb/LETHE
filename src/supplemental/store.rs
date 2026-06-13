//! M04 Supplemental Store — non-canonical derivation store
//!
//! Enforces: AppendOnly records cannot be overwritten; ManagedCache records
//! get monotonically increasing version numbers; derivedFrom must reference
//! existing observations.

use std::collections::HashMap;

use crate::domain::{
    DomainError, Mutability, ObservationId, SupplementalId, SupplementalRecord,
};
use crate::domain::supplemental::ConsentMetadata;
use crate::lake::LakeStore;
use crate::storage_api::SupplementalStorePort;

/// Versioned snapshot of a supplemental record (for ManagedCache history).
#[derive(Debug, Clone)]
pub struct VersionedRecord {
    pub version: u64,
    pub record: SupplementalRecord,
}

/// Rollback token for a successful upsert.
#[derive(Debug, Clone)]
pub struct UpsertRollback {
    pub id: SupplementalId,
    previous: Option<VersionedRecord>,
    history_len: usize,
}

/// In-memory supplemental store with mutability enforcement.
#[derive(Debug, Default)]
pub struct SupplementalStore {
    records: HashMap<String, VersionedRecord>,
    /// All historical versions for ManagedCache records.
    history: Vec<VersionedRecord>,
}

impl SupplementalStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new supplemental record.
    ///
    /// `lake` is used to verify that derivedFrom observations actually exist.
    pub fn add(
        &mut self,
        record: SupplementalRecord,
        lake: &LakeStore,
    ) -> Result<SupplementalId, DomainError> {
        self.validate_record(&record, lake)?;

        if self.records.contains_key(&record.id.0) {
            return Err(DomainError::Conflict(format!(
                "Supplemental record {} already exists",
                record.id
            )));
        }

        let id = record.id.clone();
        let ver = VersionedRecord {
            version: 1,
            record,
        };
        self.history.push(ver.clone());
        self.records.insert(id.0.clone(), ver);
        Ok(id)
    }

    pub fn upsert(
        &mut self,
        mut record: SupplementalRecord,
        lake: &LakeStore,
    ) -> Result<SupplementalId, DomainError> {
        self.validate_record(&record, lake)?;

        if let Some(entry) = self.records.get_mut(&record.id.0) {
            if entry.record.mutability == Mutability::AppendOnly {
                return Err(DomainError::Policy(crate::domain::PolicyError {
                    code: "APPEND_ONLY".into(),
                    message: format!("Record {} is AppendOnly and cannot be overwritten", record.id),
                }));
            }

            let new_version = entry.version + 1;
            record.record_version = Some(new_version.to_string());
            entry.version = new_version;
            entry.record = record.clone();
            self.history.push(entry.clone());
            return Ok(record.id);
        }

        self.add(record, lake)
    }

    /// Upsert a record and return a token that can restore the previous state.
    pub fn upsert_with_rollback(
        &mut self,
        record: SupplementalRecord,
        lake: &LakeStore,
    ) -> Result<UpsertRollback, DomainError> {
        let rollback = UpsertRollback {
            id: record.id.clone(),
            previous: self.records.get(&record.id.0).cloned(),
            history_len: self.history.len(),
        };
        self.upsert(record, lake)?;
        Ok(rollback)
    }

    /// Restore the store state captured before `upsert_with_rollback`.
    pub fn rollback_upsert(&mut self, rollback: UpsertRollback) {
        self.history.truncate(rollback.history_len);
        match rollback.previous {
            Some(previous) => {
                self.records.insert(rollback.id.0.clone(), previous);
            }
            None => {
                self.records.remove(&rollback.id.0);
            }
        }
    }

    fn validate_record(
        &self,
        record: &SupplementalRecord,
        lake: &LakeStore,
    ) -> Result<(), DomainError> {
        // Invariant 2: derivedFrom must have at least one anchor.
        if record.derived_from.observations.is_empty()
            && record.derived_from.blobs.is_empty()
            && record.derived_from.supplementals.is_empty()
        {
            return Err(DomainError::Validation(
                "derivedFrom must reference at least one input".into(),
            ));
        }

        // Invariant 4: referenced observations must exist.
        for obs_id in &record.derived_from.observations {
            if lake.get(obs_id).is_none() {
                return Err(DomainError::Validation(format!(
                    "Referenced observation {} does not exist",
                    obs_id
                )));
            }
        }

        Ok(())
    }

    /// Overwrite a ManagedCache record.  AppendOnly records will be rejected.
    pub fn update(
        &mut self,
        id: &SupplementalId,
        new_payload: serde_json::Value,
    ) -> Result<u64, DomainError> {
        let entry = self
            .records
            .get_mut(&id.0)
            .ok_or_else(|| DomainError::NotFound(format!("Supplemental {} not found", id)))?;

        // Invariant 1: AppendOnly cannot be overwritten.
        if entry.record.mutability == Mutability::AppendOnly {
            return Err(DomainError::Policy(crate::domain::PolicyError {
                code: "APPEND_ONLY".into(),
                message: format!("Record {} is AppendOnly and cannot be overwritten", id),
            }));
        }

        // Invariant 5: version must monotonically increase.
        let new_version = entry.version + 1;
        entry.record.payload = new_payload;
        entry.record.record_version = Some(new_version.to_string());
        entry.version = new_version;

        self.history.push(entry.clone());

        Ok(new_version)
    }

    /// Get the current (latest) version of a supplemental record.
    pub fn get(&self, id: &SupplementalId) -> Option<&SupplementalRecord> {
        self.records.get(&id.0).map(|v| &v.record)
    }

    /// Get a specific version of a record (for academic-pinned reads).
    pub fn get_version(&self, id: &SupplementalId, version: u64) -> Option<&SupplementalRecord> {
        self.history
            .iter()
            .find(|v| v.record.id == *id && v.version == version)
            .map(|v| &v.record)
    }

    /// Get all versions for a record.
    pub fn versions(&self, id: &SupplementalId) -> Vec<&VersionedRecord> {
        self.history.iter().filter(|v| v.record.id == *id).collect()
    }

    /// Find all records derived from a specific observation.
    pub fn by_observation(&self, obs_id: &ObservationId) -> Vec<&SupplementalRecord> {
        self.records
            .values()
            .filter(|v| v.record.derived_from.observations.contains(obs_id))
            .map(|v| &v.record)
            .collect()
    }

    /// Filter by kind.
    pub fn by_kind(&self, kind: &str) -> Vec<&SupplementalRecord> {
        self.records
            .values()
            .filter(|v| v.record.kind == kind)
            .map(|v| &v.record)
            .collect()
    }

    /// Update consent metadata (Invariant 6: record is preserved, metadata changes).
    pub fn update_consent(
        &mut self,
        id: &SupplementalId,
        consent: ConsentMetadata,
    ) -> Result<(), DomainError> {
        let entry = self
            .records
            .get_mut(&id.0)
            .ok_or_else(|| DomainError::NotFound(format!("Supplemental {} not found", id)))?;
        entry.record.consent_metadata = Some(consent);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl SupplementalStorePort for SupplementalStore {
    fn supplemental_records(&self) -> Vec<&SupplementalRecord> {
        self.records.values().map(|versioned| &versioned.record).collect()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use crate::domain::supplemental::*;
    use crate::domain::*;
    use crate::lake::LakeStore;

    fn make_lake_with_obs() -> (LakeStore, ObservationId) {
        let mut lake = LakeStore::new();
        let obs = Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:test"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: None,
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("msg:1"),
            target: None,
            payload: serde_json::json!({}),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: Some(IdempotencyKey::new("test:1")),
            meta: serde_json::json!({}),
        };
        let id = obs.id.clone();
        lake.append(obs).unwrap();
        (lake, id)
    }

    fn make_record(obs_id: &ObservationId, mutability: Mutability) -> SupplementalRecord {
        SupplementalRecord {
            id: SupplementalId::new(format!("sup:{}", uuid::Uuid::now_v7())),
            kind: "ocr-text".into(),
            derived_from: InputAnchorSet {
                observations: vec![obs_id.clone()],
                blobs: vec![],
                supplementals: vec![],
            },
            payload: serde_json::json!({"text": "hello"}),
            created_by: ActorRef::new("pipeline:ocr"),
            created_at: Utc::now(),
            mutability,
            record_version: None,
            model_version: Some("tesseract-5.0".into()),
            consent_metadata: None,
            lineage: None,
        }
    }

    #[test]
    fn add_append_only_record() {
        let (lake, obs_id) = make_lake_with_obs();
        let mut store = SupplementalStore::new();
        let rec = make_record(&obs_id, Mutability::AppendOnly);
        let id = store.add(rec, &lake).unwrap();
        assert!(store.get(&id).is_some());
    }

    #[test]
    fn append_only_cannot_be_updated() {
        let (lake, obs_id) = make_lake_with_obs();
        let mut store = SupplementalStore::new();
        let rec = make_record(&obs_id, Mutability::AppendOnly);
        let id = store.add(rec, &lake).unwrap();

        let result = store.update(&id, serde_json::json!({"text": "changed"}));
        assert!(result.is_err());
    }

    #[test]
    fn managed_cache_can_be_updated() {
        let (lake, obs_id) = make_lake_with_obs();
        let mut store = SupplementalStore::new();
        let rec = make_record(&obs_id, Mutability::ManagedCache);
        let id = store.add(rec, &lake).unwrap();

        let v2 = store
            .update(&id, serde_json::json!({"text": "updated"}))
            .unwrap();
        assert_eq!(v2, 2);

        // Version pinned read.
        let v1_rec = store.get_version(&id, 1).unwrap();
        assert_eq!(v1_rec.payload["text"], "hello");

        let v2_rec = store.get_version(&id, 2).unwrap();
        assert_eq!(v2_rec.payload["text"], "updated");
    }

    #[test]
    fn derivation_from_nonexistent_observation_rejected() {
        let lake = LakeStore::new(); // empty
        let mut store = SupplementalStore::new();
        let rec = SupplementalRecord {
            id: SupplementalId::new("sup:bad"),
            kind: "ocr-text".into(),
            derived_from: InputAnchorSet {
                observations: vec![ObservationId::new("nonexistent")],
                blobs: vec![],
                supplementals: vec![],
            },
            payload: serde_json::json!({}),
            created_by: ActorRef::new("test"),
            created_at: Utc::now(),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        };
        assert!(store.add(rec, &lake).is_err());
    }

    #[test]
    fn empty_derived_from_rejected() {
        let lake = LakeStore::new();
        let mut store = SupplementalStore::new();
        let rec = SupplementalRecord {
            id: SupplementalId::new("sup:empty"),
            kind: "ocr-text".into(),
            derived_from: InputAnchorSet::default(),
            payload: serde_json::json!({}),
            created_by: ActorRef::new("test"),
            created_at: Utc::now(),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        };
        assert!(store.add(rec, &lake).is_err());
    }

    #[test]
    fn by_observation_query() {
        let (lake, obs_id) = make_lake_with_obs();
        let mut store = SupplementalStore::new();
        let rec = make_record(&obs_id, Mutability::AppendOnly);
        store.add(rec, &lake).unwrap();

        let results = store.by_observation(&obs_id);
        assert_eq!(results.len(), 1);

        let empty = store.by_observation(&ObservationId::new("nonexistent"));
        assert!(empty.is_empty());
    }

    #[test]
    fn consent_metadata_update() {
        let (lake, obs_id) = make_lake_with_obs();
        let mut store = SupplementalStore::new();
        let rec = make_record(&obs_id, Mutability::AppendOnly);
        let id = store.add(rec, &lake).unwrap();

        store
            .update_consent(
                &id,
                ConsentMetadata {
                    referenced_observation_id: obs_id.clone(),
                    retracted_at: Some(Utc::now()),
                    opt_out_strategy: OptOutStrategy::Drop,
                    opt_out_effective_at: None,
                },
            )
            .unwrap();

        let rec = store.get(&id).unwrap();
        assert!(rec.consent_metadata.is_some());
        assert!(rec.consent_metadata.as_ref().unwrap().retracted_at.is_some());
    }

    #[test]
    fn rollback_upsert_removes_new_record() {
        let (lake, obs_id) = make_lake_with_obs();
        let mut store = SupplementalStore::new();
        let record = make_record(&obs_id, Mutability::ManagedCache);
        let rollback = store.upsert_with_rollback(record.clone(), &lake).unwrap();

        assert!(store.get(&record.id).is_some());
        store.rollback_upsert(rollback);
        assert!(store.get(&record.id).is_none());
        assert!(store.versions(&record.id).is_empty());
    }

    #[test]
    fn rollback_upsert_restores_previous_version() {
        let (lake, obs_id) = make_lake_with_obs();
        let mut store = SupplementalStore::new();
        let mut record = make_record(&obs_id, Mutability::ManagedCache);
        let id = store.add(record.clone(), &lake).unwrap();

        record.payload = serde_json::json!({"text": "updated"});
        let rollback = store.upsert_with_rollback(record, &lake).unwrap();

        assert_eq!(store.get(&id).unwrap().record_version.as_deref(), Some("2"));
        assert_eq!(store.versions(&id).len(), 2);

        store.rollback_upsert(rollback);

        assert_eq!(store.get(&id).unwrap().payload["text"], "hello");
        assert!(store.get(&id).unwrap().record_version.is_none());
        assert_eq!(store.versions(&id).len(), 1);
    }
}
