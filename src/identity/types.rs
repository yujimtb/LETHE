//! M12: Identity Resolution types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::EntityRef;
use crate::governance::types::ConfidenceLevel;

/// A single identifier from a source system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceIdentifier {
    pub source: String,
    pub identifier_type: IdentifierType,
    pub value: String,
}

/// Kind of identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierType {
    Email,
    SlackId,
    ExternalId,
    ArbitraryKey,
    UserId,
    DisplayName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityResolutionStrategy {
    pub name: String,
    pub ordered_claims: Vec<IdentifierType>,
    pub minimum_confidence: ConfidenceLevel,
}

/// A candidate person from a single source (Phase 1 output).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonCandidate {
    pub source: String,
    pub identifiers: Vec<SourceIdentifier>,
    pub display_name: Option<String>,
    pub observed_at: DateTime<Utc>,
}

/// Match type between two person candidates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchType {
    EmailExact,
    NameFuzzy,
    DomainMatch,
}

/// A potential merge between two candidates (Phase 2 output).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolutionCandidate {
    pub candidate_id: String,
    pub person_a_id: String,
    pub person_b_id: String,
    pub match_type: MatchType,
    pub confidence: ConfidenceLevel,
    pub status: CandidateStatus,
}

/// Status of a resolution candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateStatus {
    Pending,
    Accepted,
    Rejected,
}

/// A resolved person (Phase 3 output).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedPerson {
    pub person_id: EntityRef,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub identifiers: Vec<SourceIdentifier>,
    pub confidence: ConfidenceLevel,
    pub sources: Vec<String>,
    pub resolved_at: DateTime<Utc>,
    pub resolved_by: String,
}

/// The complete output of identity resolution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentityResolutionOutput {
    pub resolved_persons: Vec<ResolvedPerson>,
    pub candidates: Vec<ResolutionCandidate>,
    pub person_identifiers: Vec<PersonIdentifierRow>,
}

/// Row in the person_identifiers output table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonIdentifierRow {
    pub identifier_id: String,
    pub person_id: EntityRef,
    pub source: String,
    pub identifier_type: IdentifierType,
    pub identifier_value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_identifier_round_trips() {
        let si = SourceIdentifier {
            source: "slack".into(),
            identifier_type: IdentifierType::Email,
            value: "test@example.com".into(),
        };
        let json = serde_json::to_string(&si).unwrap();
        let back: SourceIdentifier = serde_json::from_str(&json).unwrap();
        assert_eq!(si, back);
    }

    #[test]
    fn candidate_status_round_trips() {
        for status in [
            CandidateStatus::Pending,
            CandidateStatus::Accepted,
            CandidateStatus::Rejected,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let back: CandidateStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn generalized_identifier_types_round_trip() {
        for identifier_type in [
            IdentifierType::Email,
            IdentifierType::SlackId,
            IdentifierType::ExternalId,
            IdentifierType::ArbitraryKey,
            IdentifierType::UserId,
            IdentifierType::DisplayName,
        ] {
            let json = serde_json::to_string(&identifier_type).unwrap();
            let back: IdentifierType = serde_json::from_str(&json).unwrap();
            assert_eq!(identifier_type, back);
        }
    }
}
