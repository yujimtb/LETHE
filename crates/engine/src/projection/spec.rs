//! M05: Projection Spec — declarative projection definition.

use serde::{Deserialize, Serialize};

use lethe_core::domain::{ProjectionKind, ProjectionRef, ReadMode, SchemaRef, SemVer};

/// A reference to a projection source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SourceRef {
    Lake,
    Supplemental,
    Projection {
        id: ProjectionRef,
        version: String,
    },
    SourceNative {
        system: String,
        read_mode: ReadMode,
        fallback: Option<String>,
    },
}

/// A single source declaration inside a ProjectionSpec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDecl {
    pub source: SourceRef,
    #[serde(default)]
    pub filter_schemas: Vec<SchemaRef>,
    #[serde(default)]
    pub filter_derivations: Vec<String>,
}

impl SourceDecl {
    /// Extract projection dependencies (for DAG edges).
    pub fn projection_dep(&self) -> Option<&ProjectionRef> {
        match &self.source {
            SourceRef::Projection { id, .. } => Some(id),
            _ => None,
        }
    }
}

/// Read mode policy attached to a projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadModePolicy {
    pub mode: ReadMode,
    pub source_policy: String,
}

/// Build specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildSpec {
    pub build_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    pub projector: String,
}

/// Output specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSpec {
    pub format: String,
    #[serde(default)]
    pub tables: Vec<String>,
}

/// Multi-source reconciliation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationPolicy {
    LakeFirst,
    SourceLatest,
    DualTrack,
}

/// Gap policy action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapAction {
    Warn,
    Block,
    FillNull,
}

/// The full projection specification (YAML-equivalent in Rust).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionSpec {
    pub id: ProjectionRef,
    pub name: String,
    pub version: SemVer,
    pub kind: ProjectionKind,
    pub sources: Vec<SourceDecl>,
    pub read_modes: Vec<ReadModePolicy>,
    pub build: BuildSpec,
    pub outputs: Vec<OutputSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciliation: Option<ReconciliationPolicy>,
    #[serde(default)]
    pub deterministic_in: Vec<ReadMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gap_action: Option<GapAction>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub created_by: String,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Errors detected during spec validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecValidationError {
    NoSources,
    MultiSourceWithoutReconciliation,
    AcademicPinnedWithSourceNative,
    MissingVersionPinForAcademicSupplemental,
    NoReadModes,
    NoOutputs,
}

impl std::fmt::Display for SpecValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSources => write!(f, "Projection spec must declare at least one source"),
            Self::MultiSourceWithoutReconciliation => {
                write!(f, "Multi-source spec requires reconciliation policy")
            }
            Self::AcademicPinnedWithSourceNative => {
                write!(f, "AcademicPinned read mode forbids source-native sources")
            }
            Self::MissingVersionPinForAcademicSupplemental => {
                write!(f, "AcademicPinned + supplemental requires version pin")
            }
            Self::NoReadModes => write!(f, "Projection spec must declare at least one read mode"),
            Self::NoOutputs => write!(f, "Projection spec must declare at least one output"),
        }
    }
}

impl ProjectionSpec {
    /// Validate the spec against M05 invariants.
    pub fn validate(&self) -> Result<(), Vec<SpecValidationError>> {
        let mut errors = Vec::new();

        if self.sources.is_empty() {
            errors.push(SpecValidationError::NoSources);
        }

        if self.read_modes.is_empty() {
            errors.push(SpecValidationError::NoReadModes);
        }

        if self.outputs.is_empty() {
            errors.push(SpecValidationError::NoOutputs);
        }

        // Multi-source requires reconciliation (M05 §4.3).
        let has_source_native = self
            .sources
            .iter()
            .any(|s| matches!(s.source, SourceRef::SourceNative { .. }));
        if self.sources.len() > 1 && self.reconciliation.is_none() {
            errors.push(SpecValidationError::MultiSourceWithoutReconciliation);
        }

        // AcademicPinned forbids source-native (M05 §4).
        let is_academic = self.deterministic_in.contains(&ReadMode::AcademicPinned);
        if is_academic && has_source_native {
            errors.push(SpecValidationError::AcademicPinnedWithSourceNative);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn base_spec() -> ProjectionSpec {
        ProjectionSpec {
            id: ProjectionRef::new("proj:test"),
            name: "Test".into(),
            version: SemVer::new("1.0.0"),
            kind: ProjectionKind::PureProjection,
            sources: vec![SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![SchemaRef::new("schema:slack-message")],
                filter_derivations: vec![],
            }],
            read_modes: vec![ReadModePolicy {
                mode: ReadMode::OperationalLatest,
                source_policy: "lake-latest".into(),
            }],
            build: BuildSpec {
                build_type: "rust".into(),
                entrypoint: None,
                projector: "projections/test".into(),
            },
            outputs: vec![OutputSpec {
                format: "sql".into(),
                tables: vec!["test_table".into()],
            }],
            reconciliation: None,
            deterministic_in: vec![ReadMode::AcademicPinned],
            gap_action: Some(GapAction::Warn),
            tags: vec!["test".into()],
            description: None,
            created_by: "system".into(),
        }
    }

    #[test]
    fn valid_spec_passes() {
        assert!(base_spec().validate().is_ok());
    }

    #[test]
    fn empty_sources_rejected() {
        let mut spec = base_spec();
        spec.sources.clear();
        let errs = spec.validate().unwrap_err();
        assert!(errs.contains(&SpecValidationError::NoSources));
    }

    #[test]
    fn multi_source_without_reconciliation_rejected() {
        let mut spec = base_spec();
        spec.sources.push(SourceDecl {
            source: SourceRef::SourceNative {
                system: "google-slides".into(),
                read_mode: ReadMode::OperationalLatest,
                fallback: Some("lake-snapshot".into()),
            },
            filter_schemas: vec![],
            filter_derivations: vec![],
        });
        let errs = spec.validate().unwrap_err();
        assert!(errs.contains(&SpecValidationError::MultiSourceWithoutReconciliation));
    }

    #[test]
    fn academic_pinned_with_source_native_rejected() {
        let mut spec = base_spec();
        spec.deterministic_in = vec![ReadMode::AcademicPinned];
        spec.sources.push(SourceDecl {
            source: SourceRef::SourceNative {
                system: "google-slides".into(),
                read_mode: ReadMode::OperationalLatest,
                fallback: None,
            },
            filter_schemas: vec![],
            filter_derivations: vec![],
        });
        // Also add reconciliation so we only test the academic+native error
        spec.reconciliation = Some(ReconciliationPolicy::DualTrack);
        let errs = spec.validate().unwrap_err();
        assert!(errs.contains(&SpecValidationError::AcademicPinnedWithSourceNative));
    }

    #[test]
    fn multi_source_with_reconciliation_passes() {
        let mut spec = base_spec();
        spec.deterministic_in.clear();
        spec.sources.push(SourceDecl {
            source: SourceRef::SourceNative {
                system: "google-slides".into(),
                read_mode: ReadMode::OperationalLatest,
                fallback: None,
            },
            filter_schemas: vec![],
            filter_derivations: vec![],
        });
        spec.reconciliation = Some(ReconciliationPolicy::LakeFirst);
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn lake_and_projection_without_reconciliation_rejected() {
        let mut spec = base_spec();
        spec.deterministic_in.clear();
        spec.sources.push(SourceDecl {
            source: SourceRef::Projection {
                id: ProjectionRef::new("proj:identity"),
                version: ">=1.0.0".into(),
            },
            filter_schemas: vec![],
            filter_derivations: vec![],
        });
        let errs = spec.validate().unwrap_err();
        assert!(errs.contains(&SpecValidationError::MultiSourceWithoutReconciliation));
    }

    #[test]
    fn source_decl_extracts_projection_dep() {
        let decl = SourceDecl {
            source: SourceRef::Projection {
                id: ProjectionRef::new("proj:upstream"),
                version: ">=1.0.0".into(),
            },
            filter_schemas: vec![],
            filter_derivations: vec![],
        };
        assert_eq!(decl.projection_dep().unwrap().as_str(), "proj:upstream");

        let lake_decl = SourceDecl {
            source: SourceRef::Lake,
            filter_schemas: vec![],
            filter_derivations: vec![],
        };
        assert!(lake_decl.projection_dep().is_none());
    }
}
