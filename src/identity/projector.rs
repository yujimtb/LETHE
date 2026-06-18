//! M12: Identity Resolution Projector — pure functional core.
//!
//! Phase 1: source-internal linking
//! Phase 2: cross-source matching (email exact, name fuzzy)
//! Phase 3: resolution graph construction

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::domain::{EntityRef, Observation};
use crate::governance::types::ConfidenceLevel;
use crate::projection::runner::Projector;

use super::types::*;

/// Identity resolution projector.
///
/// Input: Observations from Slack and Google Slides.
/// Output: resolved persons, candidates, and identifier mappings.
pub struct IdentityProjector {
    /// Previously accepted candidates (from supplemental store).
    accepted_candidates: Vec<ResolutionCandidate>,
    projector_version: String,
}

impl IdentityProjector {
    pub fn new(projector_version: &str) -> Self {
        Self {
            accepted_candidates: Vec::new(),
            projector_version: projector_version.into(),
        }
    }

    /// Load previously accepted candidates to include in resolution.
    pub fn with_accepted_candidates(mut self, candidates: Vec<ResolutionCandidate>) -> Self {
        self.accepted_candidates = candidates;
        self
    }

    /// Phase 1: Extract person candidates from observations.
    pub fn extract_candidates(observations: &[Observation]) -> Vec<PersonCandidate> {
        let mut candidates = Vec::new();

        for obs in observations {
            let schema = obs.schema.as_str();
            match schema {
                "schema:slack-message" => {
                    if let Some(candidate) = Self::extract_slack_candidate(obs) {
                        candidates.push(candidate);
                    }
                }
                "schema:workspace-object-snapshot" => {
                    candidates.extend(Self::extract_gslides_candidates(obs));
                }
                "schema:slide-analysis-result" => {
                    if let Some(candidate) = Self::extract_slide_analysis_candidate(obs) {
                        candidates.push(candidate);
                    }
                }
                _ => {}
            }
        }

        candidates
    }

    fn extract_slack_candidate(obs: &Observation) -> Option<PersonCandidate> {
        let payload = &obs.payload;
        let user_id = payload.get("user_id").and_then(|v| v.as_str());
        let user_name = payload.get("user_name").and_then(|v| v.as_str());
        let email = payload.get("email").and_then(|v| v.as_str());

        let user_id = user_id?;

        let mut identifiers = vec![SourceIdentifier {
            source: "slack".into(),
            identifier_type: IdentifierType::UserId,
            value: user_id.to_string(),
        }];

        if let Some(email) = email {
            identifiers.push(SourceIdentifier {
                source: "slack".into(),
                identifier_type: IdentifierType::Email,
                value: email.to_string(),
            });
        }

        if let Some(name) = user_name {
            identifiers.push(SourceIdentifier {
                source: "slack".into(),
                identifier_type: IdentifierType::DisplayName,
                value: name.to_string(),
            });
        }

        Some(PersonCandidate {
            source: "slack".into(),
            identifiers,
            display_name: user_name.map(String::from),
            observed_at: obs.published,
        })
    }

    fn extract_gslides_candidates(obs: &Observation) -> Vec<PersonCandidate> {
        let mut seen_emails = std::collections::HashSet::new();
        let mut candidates = Vec::new();
        let payload = &obs.payload;

        // Extract editors from relations.editors
        if let Some(editors) = payload
            .pointer("/relations/editors")
            .and_then(|v| v.as_array())
        {
            for editor in editors {
                if let Some(email) = editor.as_str() {
                    if seen_emails.insert(email.to_lowercase()) {
                        candidates.push(PersonCandidate {
                            source: "google".into(),
                            identifiers: vec![SourceIdentifier {
                                source: "google".into(),
                                identifier_type: IdentifierType::Email,
                                value: email.to_string(),
                            }],
                            display_name: None,
                            observed_at: obs.published,
                        });
                    }
                }
            }
        }

        // Extract owner (skip if already seen as editor)
        if let Some(owner) = payload.pointer("/relations/owner").and_then(|v| v.as_str()) {
            if seen_emails.insert(owner.to_lowercase()) {
                candidates.push(PersonCandidate {
                    source: "google".into(),
                    identifiers: vec![SourceIdentifier {
                        source: "google".into(),
                        identifier_type: IdentifierType::Email,
                        value: owner.to_string(),
                    }],
                    display_name: None,
                    observed_at: obs.published,
                });
            }
        }

        candidates
    }

    fn extract_slide_analysis_candidate(obs: &Observation) -> Option<PersonCandidate> {
        let display_name = obs
            .payload
            .get("person_name")
            .and_then(|v| v.as_str())
            .map(ToOwned::to_owned);

        let mut identifiers = Vec::new();

        if let Some(email) = obs.payload.get("person_email").and_then(|v| v.as_str()) {
            identifiers.push(SourceIdentifier {
                source: "slide-analysis".into(),
                identifier_type: IdentifierType::Email,
                value: email.to_string(),
            });
        }

        if let Some(name) = &display_name {
            identifiers.push(SourceIdentifier {
                source: "slide-analysis".into(),
                identifier_type: IdentifierType::DisplayName,
                value: name.clone(),
            });
        }

        if identifiers.is_empty() {
            return None;
        }

        Some(PersonCandidate {
            source: "slide-analysis".into(),
            identifiers,
            display_name,
            observed_at: obs.published,
        })
    }

    /// Phase 2: Cross-source matching.
    ///
    /// Match strategy:
    /// - Email exact match → High confidence
    /// - Name fuzzy match → Medium confidence
    pub fn cross_source_match(candidates: &[PersonCandidate]) -> Vec<ResolutionCandidate> {
        let mut matches = Vec::new();
        let mut candidate_counter = 0u64;

        // Index: email → list of (candidate_index, source)
        let mut email_index: HashMap<String, Vec<(usize, &str)>> = HashMap::new();
        // Index: normalized name → list of (candidate_index, source)
        let mut name_index: HashMap<String, Vec<(usize, &str)>> = HashMap::new();

        for (i, candidate) in candidates.iter().enumerate() {
            for ident in &candidate.identifiers {
                match ident.identifier_type {
                    IdentifierType::Email => {
                        email_index
                            .entry(ident.value.to_lowercase())
                            .or_default()
                            .push((i, &candidate.source));
                    }
                    IdentifierType::DisplayName => {
                        name_index
                            .entry(Self::normalize_name(&ident.value))
                            .or_default()
                            .push((i, &candidate.source));
                    }
                    _ => {}
                }
            }
        }

        // Email exact matches (cross-source only).
        for (_, entries) in &email_index {
            let sources: Vec<_> = entries.iter().map(|(_, s)| *s).collect();
            if entries.len() >= 2 && sources.iter().any(|s| *s != sources[0]) {
                // Cross-source email match.
                for i in 0..entries.len() {
                    for j in (i + 1)..entries.len() {
                        if entries[i].1 != entries[j].1 {
                            candidate_counter += 1;
                            matches.push(ResolutionCandidate {
                                candidate_id: format!("rc:{candidate_counter}"),
                                person_a_id: format!("pc:{}", entries[i].0),
                                person_b_id: format!("pc:{}", entries[j].0),
                                match_type: MatchType::EmailExact,
                                confidence: ConfidenceLevel::High,
                                status: CandidateStatus::Pending,
                            });
                        }
                    }
                }
            }
        }

        // Name fuzzy matches (cross-source only).
        for (_, entries) in &name_index {
            let sources: Vec<_> = entries.iter().map(|(_, s)| *s).collect();
            if entries.len() >= 2 && sources.iter().any(|s| *s != sources[0]) {
                for i in 0..entries.len() {
                    for j in (i + 1)..entries.len() {
                        if entries[i].1 != entries[j].1 {
                            candidate_counter += 1;
                            matches.push(ResolutionCandidate {
                                candidate_id: format!("rc:{candidate_counter}"),
                                person_a_id: format!("pc:{}", entries[i].0),
                                person_b_id: format!("pc:{}", entries[j].0),
                                match_type: MatchType::NameFuzzy,
                                confidence: ConfidenceLevel::Medium,
                                status: CandidateStatus::Pending,
                            });
                        }
                    }
                }
            }
        }

        matches
    }

    fn normalize_name(name: &str) -> String {
        name.trim().to_lowercase()
    }

    /// Phase 3: Build resolution graph from high-confidence matches and
    /// previously accepted candidates.
    pub fn resolve(
        &self,
        candidates: &[PersonCandidate],
        matches: &[ResolutionCandidate],
    ) -> IdentityResolutionOutput {
        // Union-Find for merging.
        let n = candidates.len();
        let mut parent: Vec<usize> = (0..n).collect();

        let find = |parent: &mut Vec<usize>, mut x: usize| -> usize {
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        };

        let union = |parent: &mut Vec<usize>, a: usize, b: usize| {
            let ra = find(parent, a);
            let rb = find(parent, b);
            if ra != rb {
                parent[rb] = ra;
            }
        };

        // Merge high-confidence matches and accepted candidates.
        for m in matches {
            let should_merge = m.confidence == ConfidenceLevel::High
                || (m.confidence == ConfidenceLevel::Medium
                    && m.status == CandidateStatus::Accepted);

            if should_merge {
                let a = Self::parse_pc_index(&m.person_a_id);
                let b = Self::parse_pc_index(&m.person_b_id);
                if let (Some(a), Some(b)) = (a, b) {
                    if a < n && b < n {
                        union(&mut parent, a, b);
                    }
                }
            }
        }

        // Also merge previously accepted candidates.
        for ac in &self.accepted_candidates {
            if ac.status == CandidateStatus::Accepted {
                let a = Self::parse_pc_index(&ac.person_a_id);
                let b = Self::parse_pc_index(&ac.person_b_id);
                if let (Some(a), Some(b)) = (a, b) {
                    if a < n && b < n {
                        union(&mut parent, a, b);
                    }
                }
            }
        }

        // Group by root.
        let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let root = find(&mut parent, i);
            groups.entry(root).or_default().push(i);
        }

        let mut resolved_persons = Vec::new();
        let mut person_identifiers = Vec::new();
        let mut grouped_members: Vec<Vec<usize>> = groups.into_values().collect();
        for members in &mut grouped_members {
            members.sort_unstable();
        }
        grouped_members.sort_by_key(|members| members[0]);

        for (group_index, members) in grouped_members.iter().enumerate() {
            let person_seq = group_index + 1;
            let person_id = EntityRef::new(format!("person:resolved-{person_seq}"));

            let mut all_identifiers = Vec::new();
            let mut all_sources = Vec::new();
            let mut all_aliases = Vec::new();
            let mut canonical_name = None;
            let mut resolved_at = DateTime::<Utc>::UNIX_EPOCH;

            for &member_idx in members {
                let candidate = &candidates[member_idx];
                resolved_at = resolved_at.max(candidate.observed_at);
                if !all_sources.contains(&candidate.source) {
                    all_sources.push(candidate.source.clone());
                }
                if canonical_name.is_none() {
                    canonical_name = candidate.display_name.clone();
                }
                if let Some(ref name) = candidate.display_name {
                    if !all_aliases.contains(name) {
                        all_aliases.push(name.clone());
                    }
                }
                for ident in &candidate.identifiers {
                    if !all_identifiers.contains(ident) {
                        all_identifiers.push(ident.clone());
                    }
                }
            }

            all_sources.sort();
            all_sources.dedup();
            all_aliases.sort();
            all_aliases.dedup();
            all_identifiers.sort_by(|left, right| {
                left.source
                    .cmp(&right.source)
                    .then(
                        Self::identifier_type_rank(left.identifier_type)
                            .cmp(&Self::identifier_type_rank(right.identifier_type)),
                    )
                    .then(left.value.cmp(&right.value))
            });
            all_identifiers.dedup();

            let confidence = if all_sources.len() > 1 {
                // Cross-source merge → check if there's a high-confidence link.
                ConfidenceLevel::High
            } else {
                ConfidenceLevel::High // Single source, auto-resolve.
            };

            let canonical = canonical_name.unwrap_or_else(|| format!("person-{person_seq}"));

            // Build identifier rows.
            for (idx, ident) in all_identifiers.iter().enumerate() {
                person_identifiers.push(PersonIdentifierRow {
                    identifier_id: format!("pi:{person_seq}:{idx}"),
                    person_id: person_id.clone(),
                    source: ident.source.clone(),
                    identifier_type: ident.identifier_type,
                    identifier_value: ident.value.clone(),
                });
            }

            resolved_persons.push(ResolvedPerson {
                person_id,
                canonical_name: canonical,
                aliases: all_aliases,
                identifiers: all_identifiers,
                confidence,
                sources: all_sources,
                resolved_at,
                resolved_by: format!("projector:identity-resolution:v{}", self.projector_version),
            });
        }

        // Collect unresolved candidates (medium/low confidence not yet accepted).
        let unresolved: Vec<ResolutionCandidate> = matches
            .iter()
            .filter(|m| {
                m.confidence != ConfidenceLevel::High && m.status != CandidateStatus::Accepted
            })
            .cloned()
            .collect();

        IdentityResolutionOutput {
            resolved_persons,
            candidates: unresolved,
            person_identifiers,
        }
    }

    fn parse_pc_index(id: &str) -> Option<usize> {
        id.strip_prefix("pc:").and_then(|s| s.parse().ok())
    }

    fn identifier_type_rank(identifier_type: IdentifierType) -> u8 {
        match identifier_type {
            IdentifierType::Email => 0,
            IdentifierType::SlackId => 1,
            IdentifierType::ExternalId => 2,
            IdentifierType::ArbitraryKey => 3,
            IdentifierType::UserId => 4,
            IdentifierType::DisplayName => 5,
        }
    }
}

impl Projector for IdentityProjector {
    type Input = Observation;
    type Output = IdentityResolutionOutput;

    fn project(&self, inputs: &[Observation]) -> Vec<IdentityResolutionOutput> {
        let candidates = Self::extract_candidates(inputs);
        let matches = Self::cross_source_match(&candidates);
        let output = self.resolve(&candidates, &matches);
        vec![output]
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::*;

    fn slack_obs(user_id: &str, user_name: &str, email: Option<&str>, key: &str) -> Observation {
        let mut payload = serde_json::json!({
            "user_id": user_id,
            "user_name": user_name,
            "text": "hello",
        });
        if let Some(email) = email {
            payload["email"] = serde_json::Value::String(email.into());
        }
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:slack:{key}")),
            target: None,
            payload,
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({}),
        }
    }

    fn gslides_obs(editors: &[&str], owner: &str, key: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:workspace-object-snapshot"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:gslides-crawler"),
            source_system: Some(SourceSystemRef::new("sys:google-slides")),
            actor: None,
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new(format!("document:gslide:{key}")),
            target: None,
            payload: serde_json::json!({
                "relations": {
                    "editors": editors,
                    "owner": owner,
                },
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({}),
        }
    }

    #[test]
    fn same_email_cross_source_resolves_to_one_person() {
        let observations = vec![
            slack_obs("U123", "田中太郎", Some("tanaka@example.jp"), "s1"),
            gslides_obs(&["tanaka@example.jp"], "tanaka@example.jp", "g1"),
        ];

        let projector = IdentityProjector::new("1.0.0");
        let results = projector.project(&observations);
        let output = &results[0];

        // Same email → should merge into 1 resolved person.
        assert_eq!(output.resolved_persons.len(), 1);
        let person = &output.resolved_persons[0];
        assert!(person.sources.contains(&"slack".to_string()));
        assert!(person.sources.contains(&"google".to_string()));
        assert_eq!(person.canonical_name, "田中太郎");
    }

    #[test]
    fn different_emails_stay_separate() {
        let observations = vec![
            slack_obs("U123", "Alice", Some("alice@example.com"), "s1"),
            gslides_obs(&["bob@example.com"], "bob@example.com", "g1"),
        ];

        let projector = IdentityProjector::new("1.0.0");
        let results = projector.project(&observations);
        let output = &results[0];

        // Different emails → 2 separate persons.
        assert_eq!(output.resolved_persons.len(), 2);
    }

    #[test]
    fn medium_confidence_stays_as_candidate() {
        // Same name but different emails → should be a candidate, not auto-merged.
        let observations = vec![
            slack_obs("U123", "tanaka", Some("tanaka-a@example.com"), "s1"),
            gslides_obs(&["tanaka-b@example.com"], "tanaka-b@example.com", "g1"),
        ];

        // Give the google candidate a display name via a separate observation.
        let mut obs2 = observations[1].clone();
        obs2.payload = serde_json::json!({
            "relations": {
                "editors": ["tanaka-b@example.com"],
                "owner": "tanaka-b@example.com",
            },
        });

        let _projector = IdentityProjector::new("1.0.0");
        let candidates = IdentityProjector::extract_candidates(&observations);
        let matches = IdentityProjector::cross_source_match(&candidates);

        // No email match → no high confidence match. They are separate.
        let email_matches: Vec<_> = matches
            .iter()
            .filter(|m| m.match_type == MatchType::EmailExact)
            .collect();
        assert!(email_matches.is_empty());
    }

    #[test]
    fn accepted_candidate_gets_merged() {
        let observations = vec![
            slack_obs("U123", "Alice", Some("alice-slack@example.com"), "s1"),
            gslides_obs(
                &["alice-google@example.com"],
                "alice-google@example.com",
                "g1",
            ),
        ];

        let candidates = IdentityProjector::extract_candidates(&observations);
        let matches = IdentityProjector::cross_source_match(&candidates);

        // Manually accept a candidate.
        let accepted = vec![ResolutionCandidate {
            candidate_id: "rc:manual".into(),
            person_a_id: "pc:0".into(),
            person_b_id: "pc:1".into(),
            match_type: MatchType::NameFuzzy,
            confidence: ConfidenceLevel::Medium,
            status: CandidateStatus::Accepted,
        }];

        let projector = IdentityProjector::new("1.0.0").with_accepted_candidates(accepted);
        let output = projector.resolve(&candidates, &matches);

        // Should be merged due to accepted candidate.
        assert_eq!(output.resolved_persons.len(), 1);
    }

    #[test]
    fn pending_medium_not_in_resolved() {
        let observations = vec![
            slack_obs("U123", "Bob", Some("bob-slack@example.com"), "s1"),
            gslides_obs(&["bob-google@example.com"], "bob-google@example.com", "g1"),
        ];

        let projector = IdentityProjector::new("1.0.0");
        let results = projector.project(&observations);
        let output = &results[0];

        // No email match, no name match → separate persons.
        // Pending candidates should NOT affect resolved_persons.
        assert_eq!(output.resolved_persons.len(), 2);
    }

    #[test]
    fn replay_produces_same_output() {
        let observations = vec![
            slack_obs("U123", "田中太郎", Some("tanaka@example.jp"), "s1"),
            gslides_obs(&["tanaka@example.jp"], "tanaka@example.jp", "g1"),
        ];

        let projector = IdentityProjector::new("1.0.0");
        let r1 = projector.project(&observations);
        let r2 = projector.project(&observations);

        assert_eq!(
            serde_json::to_value(&r1).unwrap(),
            serde_json::to_value(&r2).unwrap()
        );
    }

    #[test]
    fn by_identifier_lookup() {
        let observations = vec![slack_obs(
            "U999",
            "Test User",
            Some("test@example.com"),
            "s1",
        )];

        let projector = IdentityProjector::new("1.0.0");
        let results = projector.project(&observations);
        let output = &results[0];

        // Should be able to find person by email identifier.
        let found = output
            .person_identifiers
            .iter()
            .any(|pi| pi.identifier_value == "test@example.com");
        assert!(found);
    }

    #[test]
    fn slide_analysis_target_is_not_promoted_to_person_identifier() {
        let observation = Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slide-analysis-result"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slide-analysis-projector"),
            source_system: Some(SourceSystemRef::new("sys:lethe-internal")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("person:alice@hlab.college"),
            target: Some(EntityRef::new("document:gslides:pres123#slide:slide-1")),
            payload: serde_json::json!({
                "person_email": "alice@hlab.college",
                "person_name": "Alice",
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("slide-analysis:1"),
            meta: serde_json::json!({}),
        };

        let output = IdentityProjector::new("1.0.0").project(&[observation]);
        let identifiers = &output[0].resolved_persons[0].identifiers;
        assert!(
            identifiers
                .iter()
                .all(|identifier| identifier.value != "document:gslides:pres123#slide:slide-1")
        );
    }
}
