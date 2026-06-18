//! M14: Read Mode Resolver — determines the read mode for a request.

use crate::domain::ReadMode;
use crate::projection::spec::ProjectionSpec;

/// Errors from read mode resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadModeError {
    UnsupportedMode(String),
    PinRequired,
}

impl std::fmt::Display for ReadModeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedMode(mode) => write!(f, "Unsupported read mode: {mode}"),
            Self::PinRequired => write!(f, "academic-pinned mode requires ?pin= parameter"),
        }
    }
}

/// Resolve the read mode from request parameters and projection spec (M14 §3.1).
pub struct ReadModeResolver;

impl ReadModeResolver {
    /// Resolve the read mode.
    ///
    /// 1. If `requested_mode` is provided, validate against spec.
    /// 2. If absent, use the first declared read mode.
    /// 3. If `academic-pinned`, require `pin` to be present.
    pub fn resolve(
        spec: &ProjectionSpec,
        requested_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ReadMode, ReadModeError> {
        let mode = if let Some(requested) = requested_mode {
            Self::parse_mode(requested)?
        } else {
            // Default to first declared.
            spec.read_modes
                .first()
                .map(|p| p.mode)
                .unwrap_or(ReadMode::OperationalLatest)
        };

        // Validate that the spec supports this mode.
        let supported = spec.read_modes.iter().any(|p| p.mode == mode);
        if !supported {
            return Err(ReadModeError::UnsupportedMode(format!("{mode:?}")));
        }

        // academic-pinned requires pin.
        if mode == ReadMode::AcademicPinned && pin.is_none() {
            return Err(ReadModeError::PinRequired);
        }

        Ok(mode)
    }

    fn parse_mode(s: &str) -> Result<ReadMode, ReadModeError> {
        match s {
            "operational-latest" | "operational_latest" => Ok(ReadMode::OperationalLatest),
            "academic-pinned" | "academic_pinned" => Ok(ReadMode::AcademicPinned),
            "application-cached" | "application_cached" => Ok(ReadMode::ApplicationCached),
            other => Err(ReadModeError::UnsupportedMode(other.into())),
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

    fn test_spec() -> ProjectionSpec {
        ProjectionSpec {
            id: ProjectionRef::new("proj:test"),
            name: "Test".into(),
            version: SemVer::new("1.0.0"),
            kind: ProjectionKind::CachedProjection,
            sources: vec![SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![],
                filter_derivations: vec![],
            }],
            read_modes: vec![
                ReadModePolicy {
                    mode: ReadMode::OperationalLatest,
                    source_policy: "lake-latest".into(),
                },
                ReadModePolicy {
                    mode: ReadMode::AcademicPinned,
                    source_policy: "lake-pinned".into(),
                },
            ],
            build: BuildSpec {
                build_type: "rust".into(),
                entrypoint: None,
                projector: "p".into(),
            },
            outputs: vec![OutputSpec {
                format: "sql".into(),
                tables: vec!["t".into()],
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
    fn default_mode_from_spec() {
        let spec = test_spec();
        let mode = ReadModeResolver::resolve(&spec, None, None).unwrap();
        assert_eq!(mode, ReadMode::OperationalLatest);
    }

    #[test]
    fn explicit_operational_latest() {
        let spec = test_spec();
        let mode = ReadModeResolver::resolve(&spec, Some("operational-latest"), None).unwrap();
        assert_eq!(mode, ReadMode::OperationalLatest);
    }

    #[test]
    fn academic_pinned_with_pin() {
        let spec = test_spec();
        let mode =
            ReadModeResolver::resolve(&spec, Some("academic-pinned"), Some("v1.0.0")).unwrap();
        assert_eq!(mode, ReadMode::AcademicPinned);
    }

    #[test]
    fn academic_pinned_without_pin_fails() {
        let spec = test_spec();
        let result = ReadModeResolver::resolve(&spec, Some("academic-pinned"), None);
        assert_eq!(result, Err(ReadModeError::PinRequired));
    }

    #[test]
    fn unknown_mode_fails() {
        let spec = test_spec();
        let result = ReadModeResolver::resolve(&spec, Some("unknown"), None);
        assert!(matches!(result, Err(ReadModeError::UnsupportedMode(_))));
    }

    #[test]
    fn unsupported_mode_by_spec_fails() {
        let mut spec = test_spec();
        spec.read_modes = vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "lake-latest".into(),
        }];
        // AcademicPinned not in spec.
        let result = ReadModeResolver::resolve(&spec, Some("academic-pinned"), Some("v1"));
        assert!(matches!(result, Err(ReadModeError::UnsupportedMode(_))));
    }
}
