//! M03 Observation Lake — Append-only observation store
//!
//! Non-authoritative in-memory cache for appended observations.

use std::collections::HashMap;

use crate::domain::{
    EntityRef, Observation, ObservationId, ObserverRef, SchemaRef,
};
use crate::storage_api::ObservationStore;

const CANONICAL_JSON_META_KEY: &str = "canonical_json";

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

/// In-memory append-only observation cache.
#[derive(Debug, Default)]
pub struct LakeStore {
    observations: Vec<Observation>,
    /// identity_key → index in `observations`
    dedup_index: HashMap<String, usize>,
}

impl LakeStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an observation. Returns `Err(existing_id)` if the identity key
    /// was already seen.
    pub fn append(&mut self, obs: Observation) -> Result<ObservationId, ObservationId> {
        match self.append_idempotent(obs) {
            AppendOutcome::Appended(id) => Ok(id),
            AppendOutcome::Duplicate(existing_id) | AppendOutcome::Conflict(existing_id) => {
                Err(existing_id)
            }
        }
    }

    pub fn append_idempotent(&mut self, obs: Observation) -> AppendOutcome {
        let key = &obs.idempotency_key;
        if let Some(&idx) = self.dedup_index.get(&key.0) {
            let existing = &self.observations[idx];
            return if same_canonical_json(existing, &obs) {
                AppendOutcome::Duplicate(existing.id.clone())
            } else {
                AppendOutcome::Conflict(existing.id.clone())
            };
        }
        let id = obs.id.clone();
        let idx = self.observations.len();
        self.dedup_index.insert(key.0.clone(), idx);
        self.observations.push(obs);
        AppendOutcome::Appended(id)
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

fn same_canonical_json(left: &Observation, right: &Observation) -> bool {
    canonical_json(left) == canonical_json(right)
}

fn canonical_json(observation: &Observation) -> &str {
    observation
        .meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .expect("observation.meta.canonical_json is required")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    use chrono::Utc;

    fn sample_obs(key: &str) -> Observation {
        let canonical_json = serde_json::json!({
            "source": "test",
            "object_id": key,
            "body": "hello"
        })
        .to_string();
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
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({
                CANONICAL_JSON_META_KEY: canonical_json,
            }),
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
    fn duplicate_identity_key_with_different_payload_is_duplicate_when_canonical_matches() {
        let mut lake = LakeStore::new();
        let o1 = sample_obs("dup-different");
        let mut o2 = sample_obs("dup-different");
        o2.payload = serde_json::json!({"changed": true});
        let id1 = lake.append(o1).unwrap();
        let result = lake.append_idempotent(o2);
        assert_eq!(result, AppendOutcome::Duplicate(id1));
        assert_eq!(lake.len(), 1);
    }

    #[test]
    fn duplicate_identity_key_with_different_canonical_json_is_conflict() {
        let mut lake = LakeStore::new();
        let o1 = sample_obs("canonical-collision");
        let mut o2 = sample_obs("canonical-collision");
        o2.meta = serde_json::json!({
            CANONICAL_JSON_META_KEY: serde_json::json!({
                "source": "test",
                "object_id": "canonical-collision",
                "body": "changed"
            }).to_string(),
        });

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
    fn missing_canonical_json_panics() {
        let mut lake = LakeStore::new();
        let mut obs = sample_obs("missing-canonical");
        obs.meta = serde_json::json!({});
        lake.append(obs).unwrap();

        let mut dup = sample_obs("missing-canonical");
        dup.meta = serde_json::json!({});
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            lake.append_idempotent(dup);
        }));

        assert!(result.is_err());
    }
}
