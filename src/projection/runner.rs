//! M05: Projection Runner — build abstraction and result tracking.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::ProjectionRef;

use super::lineage::{LineageManifest, SourceSnapshot};
use super::spec::ProjectionSpec;

/// Status of a projection build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildStatus {
    Success,
    Partial,
    Failed,
}

/// Result of running a projection build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildResult {
    pub build_id: String,
    pub projection_id: ProjectionRef,
    pub status: BuildStatus,
    pub built_at: DateTime<Utc>,
    pub output_count: usize,
    pub lineage: LineageManifest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Trait for projector implementations.
///
/// A projector is the **pure functional core** of a projection build.
/// It receives observations and produces output records.
pub trait Projector {
    type Input;
    type Output;

    /// Apply a batch of inputs and produce outputs. Must be deterministic
    /// for `academic-pinned` read mode.
    fn project(&self, inputs: &[Self::Input]) -> Vec<Self::Output>;
}

/// The projection runner orchestrates: spec lint → input gather → project → lineage.
pub struct ProjectionRunner;

impl ProjectionRunner {
    /// Execute a build using a projector.
    ///
    /// This is a simplified MVP runner that:
    /// 1. Generates a build id
    /// 2. Calls the projector
    /// 3. Produces lineage manifest
    pub fn build<P: Projector>(
        spec: &ProjectionSpec,
        projector: &P,
        inputs: &[P::Input],
        source_snapshots: Vec<SourceSnapshot>,
    ) -> BuildResult {
        let build_id = format!("build-{}", uuid::Uuid::now_v7());
        let outputs = projector.project(inputs);
        let output_count = outputs.len();

        let mut lineage =
            LineageManifest::new(spec.id.clone(), spec.version.clone(), build_id.clone());
        for snap in source_snapshots {
            lineage.add_source(snap);
        }
        lineage.output_count = output_count;
        lineage.deterministic = spec
            .deterministic_in
            .contains(&crate::domain::ReadMode::AcademicPinned);

        BuildResult {
            build_id,
            projection_id: spec.id.clone(),
            status: BuildStatus::Success,
            built_at: Utc::now(),
            output_count,
            lineage,
            error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;
    use crate::projection::spec::*;

    /// Trivial projector that counts inputs.
    struct CountProjector;

    impl Projector for CountProjector {
        type Input = String;
        type Output = usize;

        fn project(&self, inputs: &[String]) -> Vec<usize> {
            vec![inputs.len()]
        }
    }

    fn test_spec() -> ProjectionSpec {
        ProjectionSpec {
            id: ProjectionRef::new("proj:count"),
            name: "Count".into(),
            version: SemVer::new("1.0.0"),
            kind: ProjectionKind::PureProjection,
            sources: vec![SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![],
                filter_derivations: vec![],
            }],
            read_modes: vec![ReadModePolicy {
                mode: ReadMode::OperationalLatest,
                source_policy: "lake-latest".into(),
            }],
            build: BuildSpec {
                build_type: "rust".into(),
                entrypoint: None,
                projector: "count".into(),
            },
            outputs: vec![OutputSpec {
                format: "sql".into(),
                tables: vec!["counts".into()],
            }],
            reconciliation: None,
            deterministic_in: vec![ReadMode::AcademicPinned],
            gap_action: None,
            tags: vec![],
            description: None,
            created_by: "test".into(),
        }
    }

    #[test]
    fn runner_produces_build_result() {
        let spec = test_spec();
        let projector = CountProjector;
        let inputs = vec!["a".into(), "b".into(), "c".into()];
        let snapshots = vec![SourceSnapshot {
            source_ref: "lake".into(),
            watermark_position: Some(3),
            record_count: 3,
        }];

        let result = ProjectionRunner::build(&spec, &projector, &inputs, snapshots);
        assert_eq!(result.status, BuildStatus::Success);
        assert_eq!(result.output_count, 1);
        assert_eq!(result.lineage.input_count, 3);
        assert!(result.lineage.deterministic);
    }

    #[test]
    fn replay_produces_same_output() {
        let spec = test_spec();
        let projector = CountProjector;
        let inputs: Vec<String> = vec!["x".into(), "y".into()];
        let snap = || {
            vec![SourceSnapshot {
                source_ref: "lake".into(),
                watermark_position: Some(2),
                record_count: 2,
            }]
        };

        let r1 = ProjectionRunner::build(&spec, &projector, &inputs, snap());
        let r2 = ProjectionRunner::build(&spec, &projector, &inputs, snap());
        assert_eq!(r1.output_count, r2.output_count);
    }
}
