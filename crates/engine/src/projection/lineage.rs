//! M05: Lineage Manifest — provenance tracking for builds.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use lethe_core::domain::{ProjectionRef, SemVer};

/// A snapshot of a single source consumed during a build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSnapshot {
    pub source_ref: String,
    pub watermark_position: Option<usize>,
    pub record_count: usize,
}

/// Lineage manifest generated after every build (M05 §10.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageManifest {
    pub projection_id: ProjectionRef,
    pub version: SemVer,
    pub build_id: String,
    pub built_at: DateTime<Utc>,
    pub sources: Vec<SourceSnapshot>,
    pub input_count: usize,
    pub output_count: usize,
    pub deterministic: bool,
    #[serde(default)]
    pub input_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
}

impl LineageManifest {
    /// Create a new lineage manifest for a build.
    pub fn new(projection_id: ProjectionRef, version: SemVer, build_id: String) -> Self {
        Self {
            projection_id,
            version,
            build_id,
            built_at: Utc::now(),
            sources: Vec::new(),
            input_count: 0,
            output_count: 0,
            deterministic: true,
            input_refs: Vec::new(),
            seed: None,
        }
    }

    pub fn add_source(&mut self, snapshot: SourceSnapshot) {
        self.input_count += snapshot.record_count;
        self.sources.push(snapshot);
    }

    pub fn add_input_ref(&mut self, input_ref: impl Into<String>) {
        self.input_refs.push(input_ref.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lineage_manifest_tracks_inputs() {
        let mut manifest = LineageManifest::new(
            ProjectionRef::new("proj:test"),
            SemVer::new("1.0.0"),
            "build-001".into(),
        );
        manifest.add_source(SourceSnapshot {
            source_ref: "lake".into(),
            watermark_position: Some(42),
            record_count: 10,
        });
        manifest.add_source(SourceSnapshot {
            source_ref: "supplemental".into(),
            watermark_position: None,
            record_count: 3,
        });
        assert_eq!(manifest.input_count, 13);
        assert_eq!(manifest.sources.len(), 2);
    }

    #[test]
    fn lineage_manifest_serializes() {
        let manifest = LineageManifest::new(
            ProjectionRef::new("proj:test"),
            SemVer::new("1.0.0"),
            "build-001".into(),
        );
        let json = serde_json::to_string(&manifest).unwrap();
        let back: LineageManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.projection_id, manifest.projection_id);
    }
}
