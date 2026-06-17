//! M15 Runtime — partition log and keyspec definitions.
//!
//! The first rollout starts with a single physical leaf that is also the root.
//! The complete routing and identity keyspecs are still pinned from day one.

use serde::Serialize;

pub const PARTITION_EVENT_INITIALIZE: &str = "initialize";
pub const PARTITION_EVENT_SPLIT_PREPARE: &str = "split_prepare";
pub const PARTITION_EVENT_SPLIT_COMMIT: &str = "split_commit";
pub const PARTITION_EVENT_FAILOVER: &str = "failover";
pub const PARTITION_EVENT_RECOVER: &str = "recover";

pub const ROUTING_KEYSPEC_VERSION: &str = "routing-keyspec/v1";
pub const IDENTITY_KEYSPEC_VERSION: &str = "identity-keyspec/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutingKeySpec {
    pub version: &'static str,
    pub axes: Vec<RoutingAxisSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutingAxisSpec {
    pub name: &'static str,
    pub source_field: &'static str,
    pub encoding: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentityKeySpec {
    pub version: &'static str,
    pub structure: &'static str,
    pub hash: &'static str,
    pub object_id_rule: &'static str,
    pub canonical_content: CanonicalContentSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalContentSpec {
    pub include: Vec<&'static str>,
    pub exclude: Vec<&'static str>,
    pub normalization: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InitializePartitionEvent {
    pub root_leaf_id: String,
    pub routing_keyspec_version: &'static str,
    pub identity_keyspec_version: &'static str,
}

pub fn routing_keyspec() -> RoutingKeySpec {
    RoutingKeySpec {
        version: ROUTING_KEYSPEC_VERSION,
        axes: vec![
            RoutingAxisSpec {
                name: "coarse_month",
                source_field: "published",
                encoding: "utc_month_2digit",
            },
            RoutingAxisSpec {
                name: "coarse_year",
                source_field: "published",
                encoding: "utc_year_4digit",
            },
            RoutingAxisSpec {
                name: "source",
                source_field: "source_system",
                encoding: "stable_source_ref",
            },
            RoutingAxisSpec {
                name: "container",
                source_field: "source_container",
                encoding: "adapter_declared_workspace_or_channel",
            },
            RoutingAxisSpec {
                name: "fine_published",
                source_field: "published",
                encoding: "utc_rfc3339_nanos",
            },
        ],
    }
}

pub fn identity_keyspec() -> IdentityKeySpec {
    IdentityKeySpec {
        version: IDENTITY_KEYSPEC_VERSION,
        structure: "source:object_id:sha256(canonical_content)",
        hash: "sha256",
        object_id_rule: "adapter_declared_source_specific_object_id",
        canonical_content: CanonicalContentSpec {
            include: vec!["sender", "body", "event_time", "attachment_sha256"],
            exclude: vec!["reactions", "edit_wrapper", "ingestion_meta"],
            normalization: vec![
                "unicode_nfc",
                "crlf_to_lf",
                "json_canonical",
                "rfc3339_utc_fixed_precision",
            ],
        },
    }
}

pub fn routing_keyspec_json() -> Result<String, serde_json::Error> {
    serde_json::to_string(&routing_keyspec())
}

pub fn identity_keyspec_json() -> Result<String, serde_json::Error> {
    serde_json::to_string(&identity_keyspec())
}

pub fn initialize_event_json(root_leaf_id: &str) -> Result<String, serde_json::Error> {
    serde_json::to_string(&InitializePartitionEvent {
        root_leaf_id: root_leaf_id.to_owned(),
        routing_keyspec_version: ROUTING_KEYSPEC_VERSION,
        identity_keyspec_version: IDENTITY_KEYSPEC_VERSION,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_keyspec_pins_axis_order() {
        let axes = routing_keyspec()
            .axes
            .into_iter()
            .map(|axis| axis.name)
            .collect::<Vec<_>>();

        assert_eq!(
            axes,
            vec![
                "coarse_month",
                "coarse_year",
                "source",
                "container",
                "fine_published"
            ]
        );
    }

    #[test]
    fn identity_keyspec_pins_canonical_boundary() {
        let spec = identity_keyspec();

        assert_eq!(spec.structure, "source:object_id:sha256(canonical_content)");
        assert!(spec.canonical_content.include.contains(&"body"));
        assert!(spec.canonical_content.exclude.contains(&"reactions"));
        assert!(spec.canonical_content.normalization.contains(&"unicode_nfc"));
    }
}
