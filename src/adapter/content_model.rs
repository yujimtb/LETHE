//! Adapter content-model declarations.

use serde::{Deserialize, Serialize};

pub const CDC_MERKLE_SCHEMA: &str = "schema:content-cdc-merkle";
pub const REVISIONED_SNAPSHOT_SCHEMA: &str = "schema:revisioned-snapshot";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcMerkleManifest {
    pub object_id: String,
    pub root_sha256: String,
    pub chunks: Vec<CdcMerkleChunk>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcMerkleChunk {
    pub ordinal: usize,
    pub sha256: String,
    pub byte_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentModelKind {
    RevisionedSnapshot,
    CdcMerkle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentModelDeclaration {
    pub schema: &'static str,
    pub kind: ContentModelKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentModelCoexistence {
    SameObjectDifferentSchemas,
}

pub fn cdc_merkle_declaration() -> ContentModelDeclaration {
    ContentModelDeclaration {
        schema: CDC_MERKLE_SCHEMA,
        kind: ContentModelKind::CdcMerkle,
    }
}

pub fn revisioned_snapshot_declaration() -> ContentModelDeclaration {
    ContentModelDeclaration {
        schema: REVISIONED_SNAPSHOT_SCHEMA,
        kind: ContentModelKind::RevisionedSnapshot,
    }
}

pub fn coexistence_policy(
    snapshot_object_id: &str,
    cdc_object_id: &str,
) -> Option<ContentModelCoexistence> {
    if snapshot_object_id == cdc_object_id {
        Some(ContentModelCoexistence::SameObjectDifferentSchemas)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdc_merkle_manifest_serializes() {
        let manifest = CdcMerkleManifest {
            object_id: "doc:one".to_owned(),
            root_sha256: "a".repeat(64),
            chunks: vec![CdcMerkleChunk {
                ordinal: 0,
                sha256: "b".repeat(64),
                byte_len: 128,
            }],
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let back = serde_json::from_str::<CdcMerkleManifest>(&json).unwrap();

        assert_eq!(back, manifest);
        assert_eq!(cdc_merkle_declaration().schema, CDC_MERKLE_SCHEMA);
    }

    #[test]
    fn cdc_and_revisioned_snapshot_coexist_for_same_object() {
        assert_eq!(
            coexistence_policy("presentation:one", "presentation:one"),
            Some(ContentModelCoexistence::SameObjectDifferentSchemas)
        );
        assert_eq!(coexistence_policy("presentation:one", "doc:two"), None);
    }
}
