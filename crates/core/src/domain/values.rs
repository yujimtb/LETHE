//! M01 Domain Kernel — Value Objects
//!
//! Thin wrappers that carry domain meaning and enforce formatting rules.

use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Branded string newtypes
// ---------------------------------------------------------------------------

macro_rules! branded_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

branded_id!(
    /// `{type}:{id}` — e.g. `person:tanaka-2026`, `room:A-301`
    EntityRef
);
branded_id!(
    /// `obs:{name}`
    ObserverRef
);
branded_id!(
    /// `sys:{name}`
    SourceSystemRef
);
branded_id!(
    /// `schema:{name}`
    SchemaRef
);
branded_id!(
    /// `proj:{name}`
    ProjectionRef
);
branded_id!(
    /// `blob:sha256:{hex}`
    BlobRef
);
branded_id!(
    /// `lineage:{...}`
    LineageRef
);
branded_id!(
    /// `et:{name}`
    EntityTypeRef
);
branded_id!(IdempotencyKey);
branded_id!(ConsentRef);
branded_id!(ObservationId);
branded_id!(SupplementalId);
branded_id!(CommandId);
branded_id!(ActorRef);

/// Semantic version string (e.g. "1.0.0").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SemVer(pub String);

impl SemVer {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SemVer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Maximum allowed clock skew between `published` and `recordedAt`.
pub const MAX_CLOCK_SKEW: chrono::TimeDelta = chrono::TimeDelta::minutes(10);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_ref_display() {
        let r = EntityRef::new("person:tanaka-2026");
        assert_eq!(r.to_string(), "person:tanaka-2026");
    }

    #[test]
    fn blob_ref_round_trip() {
        let r = BlobRef::new("blob:sha256:abcdef1234567890");
        let json = serde_json::to_string(&r).unwrap();
        let back: BlobRef = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
