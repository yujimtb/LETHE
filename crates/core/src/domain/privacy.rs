//! Privacy control values shared by the append-only lake and projections.

use serde::{Deserialize, Serialize};

use super::{Observation, ObservationId};

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
