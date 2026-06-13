//! M03 Observation Lake — Append-only observation store
//!
//! MVP: in-memory Vec with hash-based dedup index.

use std::collections::HashMap;

use crate::domain::{
    EntityRef, Observation, ObservationId, ObserverRef, SchemaRef,
};
use crate::storage_api::ObservationStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended(ObservationId),
    Duplicate(ObservationId),
    Conflict(ObservationId),
}

/// Watermark representing the latest position in the Lake.
#[derive(Debug, Clone)]
pub struct Watermark {
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    pub last_id: ObservationId,
    /// Index into the observations Vec (exclusive upper bound).
    pub position: usize,
}

/// In-memory append-only observation store.
#[derive(Debug, Default)]
pub struct LakeStore {
    observations: Vec<Observation>,
    /// idempotencyKey → index in `observations`
    dedup_index: HashMap<String, usize>,
}

impl LakeStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an observation. Returns `Err(existing_id)` if the idempotency key
    /// was already seen. Use `append_idempotent` when duplicate vs conflict
    /// must be distinguished.
    pub fn append(&mut self, obs: Observation) -> Result<ObservationId, ObservationId> {
        match self.append_idempotent(obs) {
            AppendOutcome::Appended(id) => Ok(id),
            AppendOutcome::Duplicate(existing_id) | AppendOutcome::Conflict(existing_id) => {
                Err(existing_id)
            }
        }
    }

    pub fn append_idempotent(&mut self, obs: Observation) -> AppendOutcome {
        if let Some(key) = &obs.idempotency_key {
            if let Some(&idx) = self.dedup_index.get(&key.0) {
                let existing = &self.observations[idx];
                return if same_idempotent_observation(existing, &obs) {
                    AppendOutcome::Duplicate(existing.id.clone())
                } else {
                    AppendOutcome::Conflict(existing.id.clone())
                };
            }
        }
        let id = obs.id.clone();
        let idx = self.observations.len();
        if let Some(key) = &obs.idempotency_key {
            self.dedup_index.insert(key.0.clone(), idx);
        }
        self.observations.push(obs);
        AppendOutcome::Appended(id)
    }

    /// Roll back the most recent append if it matches the provided id.
    ///
    /// This is only intended for failure paths where an append has not yet been
    /// durably committed outside the process.
    pub fn rollback_last_append(&mut self, id: &ObservationId) -> Option<Observation> {
        let last = self.observations.last()?;
        if last.id != *id {
            return None;
        }

        let observation = self.observations.pop()?;
        if let Some(key) = &observation.idempotency_key {
            self.dedup_index.remove(key.as_str());
        }
        Some(observation)
    }

    /// Get a single observation by id.
    pub fn get(&self, id: &ObservationId) -> Option<&Observation> {
        self.observations.iter().find(|o| o.id == *id)
    }

    /// Return all observations.
    pub fn list(&self) -> &[Observation] {
        &self.observations
    }

    /// Filter observations by schema.
    pub fn by_schema(&self, schema: &SchemaRef) -> Vec<&Observation> {
        self.observations.iter().filter(|o| o.schema == *schema).collect()
    }

    /// Filter observations by subject.
    pub fn by_subject(&self, subject: &EntityRef) -> Vec<&Observation> {
        self.observations.iter().filter(|o| o.subject == *subject).collect()
    }

    /// Filter observations by observer.
    pub fn by_observer(&self, observer: &ObserverRef) -> Vec<&Observation> {
        self.observations
            .iter()
            .filter(|o| o.observer == *observer)
            .collect()
    }

    /// Current watermark (position of last appended observation).
    pub fn watermark(&self) -> Option<Watermark> {
        self.observations.last().map(|o| Watermark {
            recorded_at: o.recorded_at,
            last_id: o.id.clone(),
            position: self.observations.len(),
        })
    }

    /// All observations appended since the given position.
    pub fn since(&self, position: usize) -> &[Observation] {
        if position >= self.observations.len() {
            &[]
        } else {
            &self.observations[position..]
        }
    }

    pub fn len(&self) -> usize {
        self.observations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.observations.is_empty()
    }
}

impl ObservationStore for LakeStore {
    fn observations(&self) -> &[Observation] {
        self.list()
    }
}

fn same_idempotent_observation(left: &Observation, right: &Observation) -> bool {
    left.schema == right.schema
        && left.schema_version == right.schema_version
        && left.observer == right.observer
        && left.source_system == right.source_system
        && left.authority_model == right.authority_model
        && left.capture_model == right.capture_model
        && left.subject == right.subject
        && left.target == right.target
        && left.payload == right.payload
        && left.attachments == right.attachments
        && left.consent == right.consent
        && left.meta == right.meta
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    use chrono::Utc;

    fn sample_obs(key: &str) -> Observation {
        Observation {
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
            idempotency_key: Some(IdempotencyKey::new(key)),
            meta: serde_json::json!({}),
        }
    }

    #[test]
    fn append_and_get() {
        let mut lake = LakeStore::new();
        let obs = sample_obs("k1");
        let id = obs.id.clone();
        assert!(lake.append(obs).is_ok());
        assert!(lake.get(&id).is_some());
        assert_eq!(lake.len(), 1);
    }

    #[test]
    fn duplicate_idempotency_key_rejected() {
        let mut lake = LakeStore::new();
        let o1 = sample_obs("dup");
        let o2 = sample_obs("dup");
        let id1 = lake.append(o1).unwrap();
        let result = lake.append(o2);
        assert_eq!(result, Err(id1));
        assert_eq!(lake.len(), 1);
    }

    #[test]
    fn duplicate_idempotency_key_with_different_payload_is_conflict() {
        let mut lake = LakeStore::new();
        let o1 = sample_obs("dup-different");
        let mut o2 = sample_obs("dup-different");
        o2.payload = serde_json::json!({"changed": true});
        let id1 = lake.append(o1).unwrap();
        let result = lake.append_idempotent(o2);
        assert_eq!(result, AppendOutcome::Conflict(id1));
        assert_eq!(lake.len(), 1);
    }

    #[test]
    fn watermark_and_since() {
        let mut lake = LakeStore::new();
        assert!(lake.watermark().is_none());

        lake.append(sample_obs("a")).unwrap();
        let wm = lake.watermark().unwrap();
        assert_eq!(wm.position, 1);

        lake.append(sample_obs("b")).unwrap();
        let since = lake.since(wm.position);
        assert_eq!(since.len(), 1);
    }

    #[test]
    fn filter_by_schema() {
        let mut lake = LakeStore::new();
        lake.append(sample_obs("x")).unwrap();
        let hits = lake.by_schema(&SchemaRef::new("schema:test"));
        assert_eq!(hits.len(), 1);
        let misses = lake.by_schema(&SchemaRef::new("schema:other"));
        assert!(misses.is_empty());
    }

    #[test]
    fn rollback_last_append_removes_latest_observation() {
        let mut lake = LakeStore::new();
        let obs = sample_obs("rollback");
        let id = obs.id.clone();
        lake.append(obs).unwrap();

        let rolled_back = lake.rollback_last_append(&id);
        assert!(rolled_back.is_some());
        assert!(lake.is_empty());
    }
}
