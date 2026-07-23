//! Privacy control values shared by the append-only lake and projections.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Observation, ObservationId};

/// The consent decision materialized by projections and the capture gate.
///
/// Consent is an append-only ledger.  The order is deliberately independent
/// of append order so that a late-arriving old decision cannot replace a newer
/// published decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsentDecision {
    pub observation_id: ObservationId,
    pub subject: String,
    pub identifier: Option<String>,
    pub status: String,
    pub published: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
}

/// The single ordering rule shared by capture and every privacy projection.
pub fn consent_decision_order(
    published: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
    observation_id: &str,
) -> (DateTime<Utc>, DateTime<Utc>, &str) {
    (published, recorded_at, observation_id)
}

pub fn consent_decision_from_observation(observation: &Observation) -> Option<ConsentDecision> {
    if observation.schema.as_str() != "schema:consent-decision" {
        return None;
    }
    let status = observation
        .payload
        .get("status")
        .and_then(serde_json::Value::as_str)?;
    Some(ConsentDecision {
        observation_id: observation.id.clone(),
        subject: observation.subject.as_str().to_owned(),
        identifier: observation
            .payload
            .get("identifier")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        status: status.to_owned(),
        published: observation.published,
        recorded_at: observation.recorded_at,
    })
}

pub fn consent_decision_keys(decision: &ConsentDecision) -> BTreeSet<String> {
    let mut keys = BTreeSet::from([decision.subject.clone()]);
    if let Some(identifier) = &decision.identifier {
        keys.insert(identifier.clone());
    }
    keys
}

/// Return the subject and identifier values whose latest consent can govern
/// an observation.  The result is unique and stable for reverse indexes.
pub fn observation_privacy_keys(observation: &Observation) -> BTreeSet<String> {
    let mut keys = BTreeSet::from([observation.subject.as_str().to_owned()]);
    for value in [
        observation.meta.get("communication_sender_id"),
        observation.payload.get("identifier"),
        observation.payload.get("user_id"),
        observation.payload.get("from"),
        observation.payload.get("author_id"),
        observation.payload.get("email"),
    ] {
        if let Some(value) = value.and_then(serde_json::Value::as_str) {
            keys.insert(value.to_owned());
        }
    }
    keys
}

/// The canonical typed target carried by a retraction observation.
///
/// A retraction is intentionally expressed in terms of an immutable
/// observation id and/or the source object's immutable id.  A subject string
/// is not a retraction target: subjects are too broad to safely erase from a
/// projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetractionTarget {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation_id: Option<ObservationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_object_id: Option<String>,
}

impl RetractionTarget {
    pub fn new(
        observation_id: Option<ObservationId>,
        source_object_id: Option<String>,
    ) -> Result<Self, String> {
        let source_object_id = source_object_id
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if observation_id.is_none() && source_object_id.is_none() {
            return Err("retraction target must contain observation_id or source_object_id".into());
        }
        Ok(Self {
            observation_id,
            source_object_id,
        })
    }

    pub fn from_value(value: &serde_json::Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "meta.retracts must be a typed object".to_owned())?;
        if let Some(key) = object
            .keys()
            .find(|key| !matches!(key.as_str(), "observation_id" | "source_object_id"))
        {
            return Err(format!("meta.retracts contains undeclared field {key}"));
        }
        let observation_id = object
            .get("observation_id")
            .map(|value| {
                serde_json::from_value(value.clone())
                    .map_err(|_| "meta.retracts.observation_id is invalid".to_owned())
            })
            .transpose()?;
        let source_object_id = object
            .get("source_object_id")
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "meta.retracts.source_object_id must be a string".to_owned())
            })
            .transpose()?;
        Self::new(observation_id, source_object_id)
    }

    pub fn matches(&self, observation: &Observation) -> bool {
        self.observation_id.as_ref() == Some(&observation.id)
            || self.source_object_id.as_deref()
                == observation
                    .meta
                    .get("object_id")
                    .and_then(serde_json::Value::as_str)
    }
}

/// Validate the optional retraction envelope on an observation.
pub fn retraction_target_from_meta(
    meta: &serde_json::Value,
) -> Result<Option<RetractionTarget>, String> {
    meta.get("retracts")
        .map(RetractionTarget::from_value)
        .transpose()
}
