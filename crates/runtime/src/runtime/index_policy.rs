//! Runtime index and placement policy.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryIndexReadMode {
    Base,
    FailFast,
    StaleWithMarker,
    ExplicitFullScan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecondaryIndexDecision {
    UseBase,
    FailFast,
    UseStaleWithMarker,
    UseExplicitFullScan,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IndexPolicyError {
    #[error("secondary index read mode must be explicit")]
    MissingExplicitReadMode,
    #[error("placement axis is resolved and cannot be used for leaf pruning: {0}")]
    ResolvedPlacementAxis(String),
}

pub fn decide_secondary_index_read(
    mode: Option<SecondaryIndexReadMode>,
    index_is_stale: bool,
) -> Result<SecondaryIndexDecision, IndexPolicyError> {
    match mode {
        None => Err(IndexPolicyError::MissingExplicitReadMode),
        Some(SecondaryIndexReadMode::Base) => Ok(SecondaryIndexDecision::UseBase),
        Some(SecondaryIndexReadMode::FailFast) if index_is_stale => {
            Ok(SecondaryIndexDecision::FailFast)
        }
        Some(SecondaryIndexReadMode::FailFast) => Ok(SecondaryIndexDecision::UseBase),
        Some(SecondaryIndexReadMode::StaleWithMarker) => {
            Ok(SecondaryIndexDecision::UseStaleWithMarker)
        }
        Some(SecondaryIndexReadMode::ExplicitFullScan) => {
            Ok(SecondaryIndexDecision::UseExplicitFullScan)
        }
    }
}

pub fn validate_placement_axes(axes: &[&str]) -> Result<(), IndexPolicyError> {
    for axis in axes {
        if matches!(*axis, "person" | "subject" | "project") {
            return Err(IndexPolicyError::ResolvedPlacementAxis((*axis).to_owned()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_secondary_index_requires_explicit_mode() {
        let err = decide_secondary_index_read(None, true).unwrap_err();

        assert_eq!(err, IndexPolicyError::MissingExplicitReadMode);
    }

    #[test]
    fn explicit_full_scan_is_visible_in_decision() {
        let decision =
            decide_secondary_index_read(Some(SecondaryIndexReadMode::ExplicitFullScan), true)
                .unwrap();

        assert_eq!(decision, SecondaryIndexDecision::UseExplicitFullScan);
    }

    #[test]
    fn placement_rejects_resolved_entity_axes() {
        let err = validate_placement_axes(&["coarse_month", "person"]).unwrap_err();

        assert_eq!(
            err,
            IndexPolicyError::ResolvedPlacementAxis("person".to_owned())
        );
    }

    #[test]
    fn placement_accepts_primitive_routing_axes() {
        validate_placement_axes(&[
            "coarse_month",
            "coarse_year",
            "source",
            "container",
            "fine_published",
        ])
        .unwrap();
    }
}
