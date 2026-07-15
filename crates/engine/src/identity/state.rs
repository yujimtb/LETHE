//! Persistent append-only identity reducer.
//!
//! High-confidence claims are indexed by normalized [`IdentifierKey`] and
//! unioned online. Component membership is merged small-to-large while public
//! person IDs remain based on the minimum durable node ID.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use lethe_core::domain::EntityRef;
use lethe_policy::governance::types::ConfidenceLevel;
use serde::{Deserialize, Serialize};

use super::types::{
    CandidateStatus, IdentifierKey, IdentityResolutionOutput, MatchType, PersonCandidate,
    PersonIdentifierRow, ResolutionCandidate, ResolvedPerson, SourceIdentifier,
};

pub type IdentityNodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityNode {
    pub node_id: IdentityNodeId,
    pub candidate: PersonCandidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityComponent {
    public_seed: IdentityNodeId,
    members: BTreeSet<IdentityNodeId>,
    identifiers: BTreeSet<SourceIdentifier>,
    sources: BTreeSet<String>,
    display_names: BTreeMap<IdentityNodeId, String>,
    resolved_at: DateTime<Utc>,
}

impl IdentityComponent {
    fn new(node: &IdentityNode) -> Self {
        let mut members = BTreeSet::new();
        members.insert(node.node_id);
        let mut identifiers = BTreeSet::new();
        identifiers.extend(node.candidate.identifiers.iter().cloned());
        let mut sources = BTreeSet::new();
        sources.insert(node.candidate.source.clone());
        let mut display_names = BTreeMap::new();
        if let Some(name) = &node.candidate.display_name {
            display_names.insert(node.node_id, name.clone());
        }
        Self {
            public_seed: node.node_id,
            members,
            identifiers,
            sources,
            display_names,
            resolved_at: node.candidate.observed_at,
        }
    }

    fn absorb(&mut self, other: Self) {
        self.public_seed = self.public_seed.min(other.public_seed);
        self.members.extend(other.members);
        self.identifiers.extend(other.identifiers);
        self.sources.extend(other.sources);
        self.display_names.extend(other.display_names);
        self.resolved_at = self.resolved_at.max(other.resolved_at);
    }

    fn update_from_node(&mut self, node: &IdentityNode) {
        self.identifiers
            .extend(node.candidate.identifiers.iter().cloned());
        self.sources.insert(node.candidate.source.clone());
        if let Some(name) = &node.candidate.display_name {
            self.display_names
                .entry(node.node_id)
                .or_insert_with(|| name.clone());
        }
        self.resolved_at = self.resolved_at.max(node.candidate.observed_at);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityState {
    nodes: Vec<IdentityNode>,
    parent: Vec<IdentityNodeId>,
    component_weight: Vec<u64>,
    components: BTreeMap<IdentityNodeId, IdentityComponent>,
    #[serde(with = "identifier_bucket_serde")]
    identifier_buckets: BTreeMap<IdentifierKey, BTreeSet<IdentityNodeId>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityApplyResult {
    pub node_id: IdentityNodeId,
    pub affected_person_ids: BTreeSet<String>,
}

impl IdentityState {
    pub fn nodes(&self) -> &[IdentityNode] {
        &self.nodes
    }

    pub fn node(&self, node_id: IdentityNodeId) -> Option<&IdentityNode> {
        usize::try_from(node_id)
            .ok()
            .and_then(|index| self.nodes.get(index))
    }

    pub fn node_for_key(&self, key: &IdentifierKey) -> Option<IdentityNodeId> {
        self.identifier_buckets
            .get(key)
            .and_then(|members| members.iter().next().copied())
    }

    pub fn root_of(&self, mut node_id: IdentityNodeId) -> Option<IdentityNodeId> {
        let mut remaining = self.parent.len();
        loop {
            let index = usize::try_from(node_id).ok()?;
            let parent = *self.parent.get(index)?;
            if parent == node_id {
                return Some(node_id);
            }
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            node_id = parent;
        }
    }

    pub fn person_id_for_node(&self, node_id: IdentityNodeId) -> Option<String> {
        let root = self.root_of(node_id)?;
        let component = self.components.get(&root)?;
        Some(person_id(component.public_seed))
    }

    pub fn resolved_person_for_node(
        &self,
        node_id: IdentityNodeId,
        projector_version: &str,
    ) -> Option<ResolvedPerson> {
        let root = self.root_of(node_id)?;
        let component = self.components.get(&root)?;
        Some(resolved_person(component, projector_version))
    }

    pub fn resolved_person(
        &self,
        person_id: &str,
        projector_version: &str,
    ) -> Option<ResolvedPerson> {
        let seed = person_seed(person_id)?;
        let root = self.root_of(seed)?;
        let component = self.components.get(&root)?;
        (component.public_seed == seed).then(|| resolved_person(component, projector_version))
    }

    pub fn component_members(&self, node_id: IdentityNodeId) -> Option<&BTreeSet<IdentityNodeId>> {
        let root = self.root_of(node_id)?;
        self.components
            .get(&root)
            .map(|component| &component.members)
    }

    pub fn component_members_for_person(
        &self,
        person_id: &str,
    ) -> Option<&BTreeSet<IdentityNodeId>> {
        let seed = person_seed(person_id)?;
        let root = self.root_of(seed)?;
        let component = self.components.get(&root)?;
        (component.public_seed == seed).then_some(&component.members)
    }

    pub fn apply_new(
        &mut self,
        mut candidate: PersonCandidate,
    ) -> Result<IdentityApplyResult, String> {
        sort_identifiers(&mut candidate.identifiers);
        validate_candidate(&candidate)?;
        let node_id = u64::try_from(self.nodes.len())
            .map_err(|_| "identity node count does not fit u64".to_owned())?;
        let node = IdentityNode { node_id, candidate };
        self.nodes.push(node.clone());
        self.parent.push(node_id);
        self.component_weight.push(1);
        self.components
            .insert(node_id, IdentityComponent::new(&node));

        let mut affected = BTreeSet::from([person_id(node_id)]);
        let identifiers = node.candidate.identifiers.clone();
        for identifier in identifiers {
            self.add_claim(node_id, identifier, &mut affected)?;
        }
        self.validate_local(node_id)?;
        Ok(IdentityApplyResult {
            node_id,
            affected_person_ids: affected,
        })
    }

    pub fn apply_update(
        &mut self,
        node_id: IdentityNodeId,
        mut candidate: PersonCandidate,
    ) -> Result<IdentityApplyResult, String> {
        sort_identifiers(&mut candidate.identifiers);
        validate_candidate(&candidate)?;
        let index = usize::try_from(node_id)
            .map_err(|_| format!("identity node {node_id} does not fit usize"))?;
        let current = self
            .nodes
            .get(index)
            .ok_or_else(|| format!("identity node {node_id} does not exist"))?;
        if current.candidate.source != candidate.source {
            return Err(format!(
                "identity node {node_id} source changed from {} to {}",
                current.candidate.source, candidate.source
            ));
        }

        let mut merged = current.candidate.clone();
        merged.observed_at = merged.observed_at.max(candidate.observed_at);
        if merged.display_name.is_none() {
            merged.display_name = candidate.display_name;
        }
        let existing_identifiers = merged
            .identifiers
            .iter()
            .map(IdentifierKey::from_identifier)
            .collect::<Result<BTreeSet<_>, _>>()?;
        merged.identifiers.extend(candidate.identifiers);
        sort_identifiers(&mut merged.identifiers);
        let new_identifiers = merged
            .identifiers
            .iter()
            .filter_map(|identifier| {
                let key = IdentifierKey::from_identifier(identifier).ok()?;
                (!existing_identifiers.contains(&key)).then_some(identifier.clone())
            })
            .collect::<Vec<_>>();
        self.nodes[index].candidate = merged;

        let root = self
            .root_of(node_id)
            .ok_or_else(|| format!("identity node {node_id} has no component"))?;
        let updated_node = self.nodes[index].clone();
        self.components
            .get_mut(&root)
            .ok_or_else(|| format!("identity root {root} has no component aggregate"))?
            .update_from_node(&updated_node);

        let mut affected = BTreeSet::from([self
            .person_id_for_node(node_id)
            .ok_or_else(|| format!("identity node {node_id} has no public person ID"))?]);
        for identifier in new_identifiers {
            self.add_claim(node_id, identifier, &mut affected)?;
        }
        self.validate_local(node_id)?;
        Ok(IdentityApplyResult {
            node_id,
            affected_person_ids: affected,
        })
    }

    fn add_claim(
        &mut self,
        node_id: IdentityNodeId,
        identifier: SourceIdentifier,
        affected: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        let key = IdentifierKey::from_identifier(&identifier)?;
        let existing = self
            .identifier_buckets
            .get(&key)
            .and_then(|members| members.iter().next().copied());
        if key.is_high_confidence() {
            if let Some(other) = existing {
                affected.insert(self.person_id_for_node(other).ok_or_else(|| {
                    format!("identity bucket member {other} has no public person ID")
                })?);
                self.union(node_id, other)?;
                affected.insert(self.person_id_for_node(node_id).ok_or_else(|| {
                    format!("identity node {node_id} has no public person ID after union")
                })?);
            }
        } else if !key.is_medium_confidence() && existing.is_some_and(|other| other != node_id) {
            return Err(format!(
                "normalized identifier {:?}/{}/{} is claimed by multiple identity nodes",
                key.identifier_type, key.namespace, key.normalized_value
            ));
        }
        self.identifier_buckets
            .entry(key)
            .or_default()
            .insert(node_id);
        Ok(())
    }

    fn union(&mut self, left: IdentityNodeId, right: IdentityNodeId) -> Result<(), String> {
        let left_root = self
            .root_of(left)
            .ok_or_else(|| format!("identity node {left} has no root"))?;
        let right_root = self
            .root_of(right)
            .ok_or_else(|| format!("identity node {right} has no root"))?;
        if left_root == right_root {
            return Ok(());
        }
        let left_index = usize::try_from(left_root)
            .map_err(|_| format!("identity root {left_root} does not fit usize"))?;
        let right_index = usize::try_from(right_root)
            .map_err(|_| format!("identity root {right_root} does not fit usize"))?;
        let left_weight = self.component_weight[left_index];
        let right_weight = self.component_weight[right_index];
        let (winner, loser) = if left_weight > right_weight
            || (left_weight == right_weight && left_root < right_root)
        {
            (left_root, right_root)
        } else {
            (right_root, left_root)
        };
        let winner_index = usize::try_from(winner)
            .map_err(|_| format!("identity root {winner} does not fit usize"))?;
        let loser_index = usize::try_from(loser)
            .map_err(|_| format!("identity root {loser} does not fit usize"))?;
        self.parent[loser_index] = winner;
        self.component_weight[winner_index] = self.component_weight[winner_index]
            .checked_add(self.component_weight[loser_index])
            .ok_or_else(|| "identity component weight overflow".to_owned())?;
        self.component_weight[loser_index] = 0;
        let loser_component = self
            .components
            .remove(&loser)
            .ok_or_else(|| format!("identity loser root {loser} has no component aggregate"))?;
        self.components
            .get_mut(&winner)
            .ok_or_else(|| format!("identity winner root {winner} has no component aggregate"))?
            .absorb(loser_component);
        Ok(())
    }

    pub fn resolution(&self, projector_version: &str) -> IdentityResolutionOutput {
        let mut components = self.components.values().collect::<Vec<_>>();
        components.sort_by_key(|component| component.public_seed);
        let mut resolved_persons = Vec::with_capacity(components.len());
        let mut person_identifiers = Vec::new();
        for component in components {
            let person = resolved_person(component, projector_version);
            for (index, identifier) in person.identifiers.iter().enumerate() {
                person_identifiers.push(PersonIdentifierRow {
                    identifier_id: format!("pi:{}:{index}", component.public_seed),
                    person_id: person.person_id.clone(),
                    source: identifier.source.clone(),
                    identifier_type: identifier.identifier_type,
                    identifier_value: identifier.value.clone(),
                });
            }
            resolved_persons.push(person);
        }

        IdentityResolutionOutput {
            resolved_persons,
            candidates: self.medium_candidates(),
            person_identifiers,
        }
    }

    pub fn members_by_person(&self) -> BTreeMap<String, BTreeSet<IdentityNodeId>> {
        self.components
            .values()
            .map(|component| (person_id(component.public_seed), component.members.clone()))
            .collect()
    }

    fn medium_candidates(&self) -> Vec<ResolutionCandidate> {
        let mut candidates = Vec::new();
        for (key, members) in &self.identifier_buckets {
            if !key.is_medium_confidence() || members.len() < 2 {
                continue;
            }
            let mut representatives = BTreeMap::<&str, IdentityNodeId>::new();
            for node_id in members {
                let source = self.nodes[*node_id as usize].candidate.source.as_str();
                representatives.entry(source).or_insert(*node_id);
            }
            let Some(anchor) = representatives.values().next().copied() else {
                continue;
            };
            for node_id in representatives.values().copied().skip(1) {
                candidates.push(ResolutionCandidate {
                    candidate_id: format!("rc:name:{anchor}:{node_id}"),
                    person_a_id: format!("pc:{anchor}"),
                    person_b_id: format!("pc:{node_id}"),
                    match_type: MatchType::NameFuzzy,
                    confidence: ConfidenceLevel::Medium,
                    status: CandidateStatus::Pending,
                });
            }
        }
        candidates
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.nodes.len() != self.parent.len() || self.nodes.len() != self.component_weight.len()
        {
            return Err("identity state vector lengths differ".to_owned());
        }
        let mut seen_members = BTreeSet::new();
        for (root, component) in &self.components {
            if self.root_of(*root) != Some(*root) {
                return Err(format!("identity component key {root} is not a root"));
            }
            if component.members.is_empty() {
                return Err(format!("identity component {root} has no members"));
            }
            if component.public_seed != *component.members.iter().next().unwrap() {
                return Err(format!(
                    "identity component {root} public seed is not minimal"
                ));
            }
            let expected_weight = u64::try_from(component.members.len())
                .map_err(|_| "identity component size does not fit u64".to_owned())?;
            if self.component_weight[*root as usize] != expected_weight {
                return Err(format!("identity component {root} weight mismatch"));
            }
            for member in &component.members {
                if self.root_of(*member) != Some(*root) {
                    return Err(format!("identity member {member} has the wrong root"));
                }
                if !seen_members.insert(*member) {
                    return Err(format!("identity member {member} appears twice"));
                }
            }
        }
        if seen_members.len() != self.nodes.len() {
            return Err("identity components do not cover every node".to_owned());
        }
        for (key, members) in &self.identifier_buckets {
            if members.is_empty() {
                return Err(format!("identity bucket {key:?} has no members"));
            }
            for node_id in members {
                let node = self
                    .node(*node_id)
                    .ok_or_else(|| format!("identity bucket references missing node {node_id}"))?;
                let present = node
                    .candidate
                    .identifiers
                    .iter()
                    .map(IdentifierKey::from_identifier)
                    .collect::<Result<BTreeSet<_>, _>>()?
                    .contains(key);
                if !present {
                    return Err(format!(
                        "identity bucket {key:?} is absent from node {node_id}"
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_local(&self, node_id: IdentityNodeId) -> Result<(), String> {
        let root = self
            .root_of(node_id)
            .ok_or_else(|| format!("identity node {node_id} has no root"))?;
        let component = self
            .components
            .get(&root)
            .ok_or_else(|| format!("identity root {root} has no component aggregate"))?;
        if !component.members.contains(&node_id) {
            return Err(format!(
                "identity component {root} does not contain node {node_id}"
            ));
        }
        Ok(())
    }
}

fn sort_identifiers(identifiers: &mut Vec<SourceIdentifier>) {
    identifiers.sort();
    identifiers.dedup();
}

fn validate_candidate(candidate: &PersonCandidate) -> Result<(), String> {
    if candidate.source.trim().is_empty() {
        return Err("identity candidate source must not be blank".to_owned());
    }
    if candidate.identifiers.is_empty() {
        return Err("identity candidate must contain an identifier".to_owned());
    }
    for identifier in &candidate.identifiers {
        IdentifierKey::from_identifier(identifier)?;
        if identifier.source.trim().to_lowercase() != candidate.source.trim().to_lowercase() {
            return Err(format!(
                "identity candidate source {} does not match identifier source {}",
                candidate.source, identifier.source
            ));
        }
    }
    Ok(())
}

fn person_id(public_seed: IdentityNodeId) -> String {
    format!("person:component-{public_seed}")
}

fn person_seed(person_id: &str) -> Option<IdentityNodeId> {
    person_id.strip_prefix("person:component-")?.parse().ok()
}

fn resolved_person(component: &IdentityComponent, projector_version: &str) -> ResolvedPerson {
    let public_id = person_id(component.public_seed);
    let canonical_name = component
        .display_names
        .iter()
        .next()
        .map(|(_, name)| name.clone())
        .unwrap_or_else(|| format!("person-{}", component.public_seed));
    let aliases = component
        .display_names
        .values()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    ResolvedPerson {
        person_id: EntityRef::new(&public_id),
        canonical_name,
        aliases,
        identifiers: component.identifiers.iter().cloned().collect(),
        confidence: ConfidenceLevel::High,
        sources: component.sources.iter().cloned().collect(),
        resolved_at: component.resolved_at,
        resolved_by: format!("projector:identity-resolution:v{projector_version}"),
    }
}

mod identifier_bucket_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(
        buckets: &BTreeMap<IdentifierKey, BTreeSet<IdentityNodeId>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        buckets.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<IdentifierKey, BTreeSet<IdentityNodeId>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(IdentifierKey, BTreeSet<IdentityNodeId>)>::deserialize(deserializer)?;
        let mut buckets = BTreeMap::new();
        for (key, members) in entries {
            if buckets.insert(key, members).is_some() {
                return Err(serde::de::Error::custom(
                    "identity state contains a duplicate identifier bucket",
                ));
            }
        }
        Ok(buckets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::types::IdentifierType;

    fn candidate(source: &str, email: &str, name: &str, second: i64) -> PersonCandidate {
        PersonCandidate {
            source: source.to_owned(),
            identifiers: vec![
                SourceIdentifier {
                    source: source.to_owned(),
                    identifier_type: IdentifierType::Email,
                    value: email.to_owned(),
                },
                SourceIdentifier {
                    source: source.to_owned(),
                    identifier_type: IdentifierType::DisplayName,
                    value: name.to_owned(),
                },
            ],
            display_name: Some(name.to_owned()),
            observed_at: DateTime::<Utc>::from_timestamp(second, 0).unwrap(),
        }
    }

    #[test]
    fn normalized_email_bucket_unions_without_pairwise_edges() {
        let mut state = IdentityState::default();
        state
            .apply_new(candidate("slack", "USER@example.test", "User", 1))
            .unwrap();
        state
            .apply_new(candidate("google", " user@EXAMPLE.test ", "Other", 2))
            .unwrap();
        state.validate().unwrap();
        let output = state.resolution("1.0.0");
        assert_eq!(output.resolved_persons.len(), 1);
        assert_eq!(
            output.resolved_persons[0].person_id.as_str(),
            "person:component-0"
        );
    }

    #[test]
    fn small_to_large_root_is_separate_from_public_seed() {
        let mut state = IdentityState::default();
        let first = state
            .apply_new(candidate("slack", "first@example.test", "First", 1))
            .unwrap()
            .node_id;
        let second = state
            .apply_new(candidate("google", "second@example.test", "Second", 2))
            .unwrap()
            .node_id;
        state
            .apply_new(candidate(
                "slide-analysis",
                "second@example.test",
                "Second",
                3,
            ))
            .unwrap();
        state
            .apply_update(first, candidate("slack", "second@example.test", "First", 4))
            .unwrap();
        assert_eq!(
            state.person_id_for_node(second).as_deref(),
            Some("person:component-0")
        );
        assert_ne!(state.root_of(first), Some(first));
        state.validate().unwrap();
    }

    #[test]
    fn medium_bucket_is_linear_star_not_all_pairs() {
        let mut state = IdentityState::default();
        for (source, email) in [
            ("slack", "a@example.test"),
            ("google", "b@example.test"),
            ("slide-analysis", "c@example.test"),
        ] {
            state
                .apply_new(candidate(source, email, "Same Name", 1))
                .unwrap();
        }
        let output = state.resolution("1.0.0");
        assert_eq!(output.candidates.len(), 2);
    }

    #[test]
    fn persisted_state_continues_with_the_same_partition_as_full_replay() {
        let prefix = [
            candidate("slack", "a@example.test", "A", 1),
            candidate("google", "b@example.test", "B", 2),
            candidate("slides", "a@example.test", "A2", 3),
        ];
        let suffix = [
            candidate("discord", "c@example.test", "C", 4),
            candidate("github", "b@example.test", "B2", 5),
        ];

        let mut resumed = IdentityState::default();
        for value in &prefix {
            resumed.apply_new(value.clone()).unwrap();
        }
        let encoded = serde_json::to_vec(&resumed).unwrap();
        let mut resumed: IdentityState = serde_json::from_slice(&encoded).unwrap();
        for value in &suffix {
            resumed.apply_new(value.clone()).unwrap();
        }

        let mut replayed = IdentityState::default();
        for value in prefix.iter().chain(&suffix) {
            replayed.apply_new(value.clone()).unwrap();
        }
        resumed.validate().unwrap();
        replayed.validate().unwrap();
        assert_eq!(
            serde_json::to_value(resumed.resolution("1.0.0")).unwrap(),
            serde_json::to_value(replayed.resolution("1.0.0")).unwrap()
        );
        assert_eq!(
            serde_json::to_value(resumed).unwrap(),
            serde_json::to_value(replayed).unwrap()
        );
    }

    #[test]
    fn balanced_unions_keep_parent_depth_logarithmic() {
        const NODE_COUNT: usize = 64;
        let mut state = IdentityState::default();
        for index in 0..NODE_COUNT {
            state
                .apply_new(candidate(
                    &format!("source-{index}"),
                    &format!("user-{index}@example.test"),
                    &format!("User {index}"),
                    i64::try_from(index).unwrap(),
                ))
                .unwrap();
        }
        let mut width = 1;
        while width < NODE_COUNT {
            for base in (0..NODE_COUNT).step_by(width * 2) {
                let right = base + width;
                state
                    .apply_update(
                        u64::try_from(right).unwrap(),
                        candidate(
                            &format!("source-{right}"),
                            &format!("user-{base}@example.test"),
                            &format!("User {right}"),
                            i64::try_from(NODE_COUNT + width + base).unwrap(),
                        ),
                    )
                    .unwrap();
            }
            width *= 2;
        }
        state.validate().unwrap();
        assert_eq!(state.resolution("1.0.0").resolved_persons.len(), 1);
        let maximum_depth = (0..NODE_COUNT)
            .map(|index| {
                let mut node = u64::try_from(index).unwrap();
                let mut depth = 0;
                while state.parent[usize::try_from(node).unwrap()] != node {
                    node = state.parent[usize::try_from(node).unwrap()];
                    depth += 1;
                }
                depth
            })
            .max()
            .unwrap();
        assert!(
            maximum_depth <= 6,
            "maximum parent depth was {maximum_depth}"
        );
    }
}
