//! Claim queue and decision ledger projection over supplemental records.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use lethe_core::domain::supplemental::InputAnchorSet;
use lethe_core::domain::{ActorRef, ProjectionRef, SupplementalId, SupplementalRecord};
use lethe_engine::projection::runner::Projector;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use unicode_normalization::UnicodeNormalization;

pub const CLAIM_QUEUE_PROJECTION_ID: &str = "proj:claim-queue";
pub const CLAIM_KIND: &str = "claim@1";
pub const CLAIM_TRANSITION_KIND: &str = "claim-transition@1";
pub const VERIFICATION_RESULT_KIND: &str = "verification-result@1";
pub const DECISION_KIND: &str = "decision@1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimState {
    Open,
    Dispatched,
    Verified,
    Refuted,
    Inconclusive,
    Terminated,
    Parked,
}

impl ClaimState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Dispatched => "dispatched",
            Self::Verified => "verified",
            Self::Refuted => "refuted",
            Self::Inconclusive => "inconclusive",
            Self::Terminated => "terminated",
            Self::Parked => "parked",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "open" => Some(Self::Open),
            "dispatched" => Some(Self::Dispatched),
            "verified" => Some(Self::Verified),
            "refuted" => Some(Self::Refuted),
            "inconclusive" => Some(Self::Inconclusive),
            "terminated" => Some(Self::Terminated),
            "parked" => Some(Self::Parked),
            _ => None,
        }
    }
}

impl std::fmt::Display for ClaimState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimStateEvent {
    pub record_id: SupplementalId,
    pub kind: String,
    pub at: DateTime<Utc>,
    pub from: ClaimState,
    pub to: ClaimState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimView {
    pub representative_id: SupplementalId,
    #[serde(default)]
    pub absorbed_ids: Vec<SupplementalId>,
    pub kind: String,
    pub derived_from: InputAnchorSet,
    pub source_refs: Vec<String>,
    pub payload_hash: String,
    pub project: String,
    pub statement: String,
    pub verification_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_quote: Option<String>,
    pub backfill: bool,
    pub state: ClaimState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub state_history: Vec<ClaimStateEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimGroup {
    pub group_id: String,
    pub source_refs: Vec<String>,
    pub members: Vec<ClaimView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionView {
    pub id: SupplementalId,
    pub project: String,
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    #[serde(default)]
    pub alternatives: Vec<String>,
    #[serde(default)]
    pub supersedes: Vec<SupplementalId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<SupplementalId>,
    pub derived_from: InputAnchorSet,
    pub created_by: ActorRef,
    pub created_at: DateTime<Utc>,
    #[serde(skip)]
    pub search_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionAuditEvent {
    pub record_id: SupplementalId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_claim_id: Option<SupplementalId>,
    pub code: String,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClaimQueueProjection {
    pub claims: Vec<ClaimView>,
    pub groups: Vec<ClaimGroup>,
    pub decisions: Vec<DecisionView>,
    #[serde(default)]
    pub audit_log: Vec<ProjectionAuditEvent>,
}

impl ClaimQueueProjection {
    pub fn groups_matching_state(&self, state: Option<ClaimState>) -> Vec<ClaimGroup> {
        self.groups_matching(state, None)
    }

    pub fn groups_matching(
        &self,
        state: Option<ClaimState>,
        backfill: Option<bool>,
    ) -> Vec<ClaimGroup> {
        self.groups
            .iter()
            .filter_map(|group| {
                let members = group
                    .members
                    .iter()
                    .filter(|claim| state.is_none_or(|state| claim.state == state))
                    .filter(|claim| backfill.is_none_or(|backfill| claim.backfill == backfill))
                    .cloned()
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    None
                } else {
                    Some(ClaimGroup {
                        group_id: group.group_id.clone(),
                        source_refs: group.source_refs.clone(),
                        members,
                    })
                }
            })
            .collect()
    }

    pub fn search_decisions(&self, query: &str, limit: usize) -> Vec<DecisionView> {
        let query = normalize_search_text(query);
        self.decisions
            .iter()
            .filter(|decision| {
                normalize_search_text(&format!(
                    "{}\n{}",
                    decision.statement,
                    decision.rationale.as_deref().unwrap_or("")
                ))
                .contains(&query)
            })
            .take(limit)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Default, Clone)]
pub struct ClaimQueueProjector;

impl ClaimQueueProjector {
    pub fn project_records(&self, inputs: &[SupplementalRecord]) -> ClaimQueueProjection {
        let mut records = inputs.to_vec();
        sort_records_observation_order(&mut records);
        let by_id = records
            .iter()
            .cloned()
            .map(|record| (record.id.as_str().to_owned(), record))
            .collect::<HashMap<_, _>>();

        let mut audit_log = Vec::new();
        let claim_accumulators = deduplicate_claims(&records, &by_id, &mut audit_log);
        let id_to_representative = claim_id_map(&claim_accumulators);
        let state_accumulators = fold_claim_states(
            &records,
            &claim_accumulators,
            &id_to_representative,
            &mut audit_log,
        );
        let claims = claim_views(claim_accumulators, state_accumulators);
        let groups = claim_groups(&claims);
        let decisions = decision_views(&records, &mut audit_log);

        ClaimQueueProjection {
            claims,
            groups,
            decisions,
            audit_log,
        }
    }
}

impl Projector for ClaimQueueProjector {
    type Input = SupplementalRecord;
    type Output = ClaimQueueProjection;

    fn project(&self, inputs: &[Self::Input]) -> Vec<Self::Output> {
        vec![self.project_records(inputs)]
    }
}

pub fn projection_ref() -> ProjectionRef {
    ProjectionRef::new(CLAIM_QUEUE_PROJECTION_ID)
}

pub fn projection_watermark(records: &[SupplementalRecord]) -> String {
    let latest = records
        .iter()
        .map(|record| record.created_at)
        .max()
        .map(|created_at| created_at.to_rfc3339())
        .unwrap_or_else(|| "empty".to_owned());
    format!("{CLAIM_QUEUE_PROJECTION_ID}:{latest}:{}", records.len())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ClaimDedupKey {
    kind: String,
    derived_from_set: Vec<String>,
    payload_hash: String,
}

#[derive(Debug, Clone)]
struct ClaimAccumulator {
    representative: SupplementalRecord,
    absorbed: Vec<SupplementalId>,
    source_refs: Vec<String>,
    payload_hash: String,
    project: String,
    statement: String,
    verification_mode: String,
    context: Option<String>,
    source_quote: Option<String>,
    backfill: bool,
}

#[derive(Debug, Clone)]
struct ClaimStateAccumulator {
    state: ClaimState,
    updated_at: DateTime<Utc>,
    history: Vec<ClaimStateEvent>,
}

fn deduplicate_claims(
    records: &[SupplementalRecord],
    by_id: &HashMap<String, SupplementalRecord>,
    audit_log: &mut Vec<ProjectionAuditEvent>,
) -> Vec<ClaimAccumulator> {
    let mut by_key: BTreeMap<ClaimDedupKey, ClaimAccumulator> = BTreeMap::new();
    for record in records.iter().filter(|record| record.kind == CLAIM_KIND) {
        let Some(statement) = string_field(&record.payload, "statement") else {
            audit_log.push(audit_event(
                record,
                None,
                "malformed_claim",
                "claim@1 payload is missing statement",
            ));
            continue;
        };
        let Some(verification_mode) = string_field(&record.payload, "verification_mode") else {
            audit_log.push(audit_event(
                record,
                None,
                "malformed_claim",
                "claim@1 payload is missing verification_mode",
            ));
            continue;
        };
        let payload_hash = sha256_hex(&canonical_json(&record.payload));
        let key = ClaimDedupKey {
            kind: record.kind.clone(),
            derived_from_set: anchor_refs(&record.derived_from),
            payload_hash: payload_hash.clone(),
        };
        if let Some(existing) = by_key.get_mut(&key) {
            existing.absorbed.push(record.id.clone());
        } else {
            by_key.insert(
                key,
                ClaimAccumulator {
                    representative: record.clone(),
                    absorbed: Vec::new(),
                    source_refs: group_source_refs(&record.derived_from, by_id),
                    payload_hash,
                    project: string_field(&record.payload, "project")
                        .unwrap_or("uncategorized")
                        .to_owned(),
                    statement: statement.to_owned(),
                    verification_mode: verification_mode.to_owned(),
                    context: string_field(&record.payload, "context").map(str::to_owned),
                    source_quote: string_field(&record.payload, "source_quote").map(str::to_owned),
                    backfill: bool_field(&record.payload, "backfill").unwrap_or(false),
                },
            );
        }
    }
    by_key.into_values().collect()
}

fn claim_id_map(claims: &[ClaimAccumulator]) -> HashMap<String, SupplementalId> {
    let mut map = HashMap::new();
    for claim in claims {
        let representative = claim.representative.id.clone();
        map.insert(representative.as_str().to_owned(), representative.clone());
        for absorbed in &claim.absorbed {
            map.insert(absorbed.as_str().to_owned(), representative.clone());
        }
    }
    map
}

fn fold_claim_states(
    records: &[SupplementalRecord],
    claims: &[ClaimAccumulator],
    id_to_representative: &HashMap<String, SupplementalId>,
    audit_log: &mut Vec<ProjectionAuditEvent>,
) -> HashMap<String, ClaimStateAccumulator> {
    let mut states = claims
        .iter()
        .map(|claim| {
            (
                claim.representative.id.as_str().to_owned(),
                ClaimStateAccumulator {
                    state: ClaimState::Open,
                    updated_at: claim.representative.created_at,
                    history: Vec::new(),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    for record in records.iter().filter(|record| {
        record.kind == CLAIM_TRANSITION_KIND || record.kind == VERIFICATION_RESULT_KIND
    }) {
        let target_representatives = record
            .derived_from
            .supplementals
            .iter()
            .filter_map(|id| {
                id_to_representative
                    .get(id.as_str())
                    .map(|representative| representative.as_str().to_owned())
            })
            .collect::<BTreeSet<_>>();
        if target_representatives.is_empty() {
            audit_log.push(audit_event(
                record,
                None,
                "missing_claim_anchor",
                "state event does not anchor a known claim",
            ));
            continue;
        }

        let to_state = match event_target_state(record) {
            Ok(state) => state,
            Err(message) => {
                for target in target_representatives {
                    audit_log.push(audit_event(
                        record,
                        Some(SupplementalId::new(target)),
                        "invalid_state_event",
                        &message,
                    ));
                }
                continue;
            }
        };

        for target in target_representatives {
            let Some(accumulator) = states.get_mut(&target) else {
                continue;
            };
            let from_state = accumulator.state;
            if transition_allowed(from_state, to_state) {
                accumulator.state = to_state;
                accumulator.updated_at = record.created_at;
                accumulator.history.push(ClaimStateEvent {
                    record_id: record.id.clone(),
                    kind: record.kind.clone(),
                    at: record.created_at,
                    from: from_state,
                    to: to_state,
                });
            } else {
                audit_log.push(audit_event(
                    record,
                    Some(SupplementalId::new(target)),
                    "invalid_transition",
                    &format!("cannot transition from {from_state} to {to_state}"),
                ));
            }
        }
    }

    states
}

fn event_target_state(record: &SupplementalRecord) -> Result<ClaimState, String> {
    match record.kind.as_str() {
        CLAIM_TRANSITION_KIND => {
            let Some(to_state) = string_field(&record.payload, "to_state") else {
                return Err("claim-transition@1 payload is missing to_state".to_owned());
            };
            ClaimState::parse(to_state)
                .ok_or_else(|| format!("claim-transition@1 has invalid to_state {to_state}"))
        }
        VERIFICATION_RESULT_KIND => {
            let Some(verdict) = string_field(&record.payload, "verdict") else {
                return Err("verification-result@1 payload is missing verdict".to_owned());
            };
            match verdict {
                "consistent" => Ok(ClaimState::Verified),
                "inconsistent" => Ok(ClaimState::Refuted),
                "inconclusive" => Ok(ClaimState::Inconclusive),
                other => Err(format!("verification-result@1 has invalid verdict {other}")),
            }
        }
        other => Err(format!("unsupported state event kind {other}")),
    }
}

fn transition_allowed(from: ClaimState, to: ClaimState) -> bool {
    if from == to {
        return true;
    }
    match from {
        ClaimState::Open => matches!(
            to,
            ClaimState::Dispatched
                | ClaimState::Verified
                | ClaimState::Refuted
                | ClaimState::Inconclusive
                | ClaimState::Terminated
                | ClaimState::Parked
        ),
        ClaimState::Dispatched => matches!(
            to,
            ClaimState::Verified
                | ClaimState::Refuted
                | ClaimState::Inconclusive
                | ClaimState::Terminated
                | ClaimState::Parked
        ),
        ClaimState::Inconclusive => matches!(
            to,
            ClaimState::Dispatched | ClaimState::Terminated | ClaimState::Parked
        ),
        ClaimState::Parked => matches!(
            to,
            ClaimState::Open | ClaimState::Dispatched | ClaimState::Terminated
        ),
        ClaimState::Verified | ClaimState::Refuted | ClaimState::Terminated => false,
    }
}

fn claim_views(
    claims: Vec<ClaimAccumulator>,
    states: HashMap<String, ClaimStateAccumulator>,
) -> Vec<ClaimView> {
    let mut views = claims
        .into_iter()
        .map(|claim| {
            let state = states
                .get(claim.representative.id.as_str())
                .cloned()
                .unwrap_or(ClaimStateAccumulator {
                    state: ClaimState::Open,
                    updated_at: claim.representative.created_at,
                    history: Vec::new(),
                });
            ClaimView {
                representative_id: claim.representative.id.clone(),
                absorbed_ids: claim.absorbed,
                kind: claim.representative.kind.clone(),
                derived_from: claim.representative.derived_from.clone(),
                source_refs: claim.source_refs,
                payload_hash: claim.payload_hash,
                project: claim.project,
                statement: claim.statement,
                verification_mode: claim.verification_mode,
                context: claim.context,
                source_quote: claim.source_quote,
                backfill: claim.backfill,
                state: state.state,
                created_at: claim.representative.created_at,
                updated_at: state.updated_at,
                state_history: state.history,
            }
        })
        .collect::<Vec<_>>();
    views.sort_by(|left, right| {
        left.created_at.cmp(&right.created_at).then_with(|| {
            left.representative_id
                .as_str()
                .cmp(right.representative_id.as_str())
        })
    });
    views
}

fn claim_groups(claims: &[ClaimView]) -> Vec<ClaimGroup> {
    let mut by_source_refs: BTreeMap<Vec<String>, Vec<ClaimView>> = BTreeMap::new();
    for claim in claims {
        by_source_refs
            .entry(claim.source_refs.clone())
            .or_default()
            .push(claim.clone());
    }
    let mut groups = by_source_refs
        .into_iter()
        .map(|(source_refs, mut members)| {
            members.sort_by(|left, right| {
                left.created_at.cmp(&right.created_at).then_with(|| {
                    left.representative_id
                        .as_str()
                        .cmp(right.representative_id.as_str())
                })
            });
            ClaimGroup {
                group_id: group_id(&source_refs),
                source_refs,
                members,
            }
        })
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        let left_created_at = left.members.first().map(|member| member.created_at);
        let right_created_at = right.members.first().map(|member| member.created_at);
        left_created_at
            .cmp(&right_created_at)
            .then_with(|| left.group_id.cmp(&right.group_id))
    });
    groups
}

fn decision_views(
    records: &[SupplementalRecord],
    audit_log: &mut Vec<ProjectionAuditEvent>,
) -> Vec<DecisionView> {
    let mut decisions = records
        .iter()
        .filter(|record| record.kind == DECISION_KIND)
        .filter_map(|record| {
            let Some(statement) = string_field(&record.payload, "statement") else {
                audit_log.push(audit_event(
                    record,
                    None,
                    "malformed_decision",
                    "decision@1 payload is missing statement",
                ));
                return None;
            };
            let rationale = string_field(&record.payload, "rationale").map(str::to_owned);
            let alternatives = string_array_field(&record.payload, "alternatives");
            let supersedes = supplemental_id_array_field(&record.payload, "supersedes");
            Some(DecisionView {
                id: record.id.clone(),
                project: string_field(&record.payload, "project")
                    .unwrap_or("uncategorized")
                    .to_owned(),
                statement: statement.to_owned(),
                search_text: normalize_search_text(&format!(
                    "{}\n{}",
                    statement,
                    rationale.as_deref().unwrap_or("")
                )),
                rationale,
                alternatives,
                supersedes,
                superseded_by: None,
                derived_from: record.derived_from.clone(),
                created_by: record.created_by.clone(),
                created_at: record.created_at,
            })
        })
        .collect::<Vec<_>>();

    let replacement_map = decision_replacement_map(&decisions, audit_log);
    for decision in &mut decisions {
        decision.superseded_by = current_replacement(
            &decision.id,
            decision.created_at,
            &replacement_map,
            audit_log,
        );
    }
    decisions.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
    decisions
}

fn decision_replacement_map(
    decisions: &[DecisionView],
    audit_log: &mut Vec<ProjectionAuditEvent>,
) -> HashMap<String, SupplementalId> {
    let mut candidates: BTreeMap<String, Vec<&DecisionView>> = BTreeMap::new();
    for decision in decisions {
        for superseded in &decision.supersedes {
            candidates
                .entry(superseded.as_str().to_owned())
                .or_default()
                .push(decision);
        }
    }

    candidates
        .into_iter()
        .filter_map(|(superseded, mut replacements)| {
            replacements.sort_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.as_str().cmp(right.id.as_str()))
            });
            if replacements.len() > 1 {
                if let Some(last) = replacements.last() {
                    audit_log.push(ProjectionAuditEvent {
                        record_id: last.id.clone(),
                        target_claim_id: None,
                        code: "ambiguous_decision_supersedes".to_owned(),
                        message: format!(
                            "multiple decisions supersede {}; newest replacement was selected",
                            superseded
                        ),
                        created_at: last.created_at,
                    });
                }
            }
            replacements
                .last()
                .map(|replacement| (superseded, replacement.id.clone()))
        })
        .collect()
}

fn current_replacement(
    id: &SupplementalId,
    created_at: DateTime<Utc>,
    replacement_map: &HashMap<String, SupplementalId>,
    audit_log: &mut Vec<ProjectionAuditEvent>,
) -> Option<SupplementalId> {
    let mut seen = HashSet::new();
    let mut current = id.clone();
    let mut replacement = None;
    while let Some(next) = replacement_map.get(current.as_str()) {
        if !seen.insert(current.as_str().to_owned()) {
            audit_log.push(ProjectionAuditEvent {
                record_id: id.clone(),
                target_claim_id: None,
                code: "decision_supersedes_cycle".to_owned(),
                message: "decision supersedes chain contains a cycle".to_owned(),
                created_at,
            });
            return None;
        }
        replacement = Some(next.clone());
        current = next.clone();
    }
    replacement
}

fn group_source_refs(
    anchors: &InputAnchorSet,
    by_id: &HashMap<String, SupplementalRecord>,
) -> Vec<String> {
    let mut observation_refs = BTreeSet::new();
    let mut visited_supplementals = HashSet::new();
    collect_observation_roots(
        anchors,
        by_id,
        &mut visited_supplementals,
        &mut observation_refs,
    );
    if !observation_refs.is_empty() {
        return observation_refs.into_iter().collect();
    }
    anchor_refs(anchors)
}

fn collect_observation_roots(
    anchors: &InputAnchorSet,
    by_id: &HashMap<String, SupplementalRecord>,
    visited_supplementals: &mut HashSet<String>,
    observation_refs: &mut BTreeSet<String>,
) {
    for observation in &anchors.observations {
        observation_refs.insert(format!("observation:{}", observation.as_str()));
    }
    for supplemental in &anchors.supplementals {
        if !visited_supplementals.insert(supplemental.as_str().to_owned()) {
            continue;
        }
        if let Some(record) = by_id.get(supplemental.as_str()) {
            collect_observation_roots(
                &record.derived_from,
                by_id,
                visited_supplementals,
                observation_refs,
            );
        }
    }
}

fn anchor_refs(anchors: &InputAnchorSet) -> Vec<String> {
    let mut refs = BTreeSet::new();
    for observation in &anchors.observations {
        refs.insert(format!("observation:{}", observation.as_str()));
    }
    for blob in &anchors.blobs {
        refs.insert(format!("blob:{}", blob.as_str()));
    }
    for supplemental in &anchors.supplementals {
        refs.insert(format!("supplemental:{}", supplemental.as_str()));
    }
    refs.into_iter().collect()
}

fn group_id(source_refs: &[String]) -> String {
    format!(
        "claim-group:{}",
        sha256_hex(&canonical_string_array(source_refs))
    )
}

fn canonical_string_array(values: &[String]) -> String {
    let mut output = String::from("[");
    for (idx, value) in values.iter().enumerate() {
        if idx > 0 {
            output.push(',');
        }
        output.push_str(&serde_json::to_string(value).expect("string serialization cannot fail"));
    }
    output.push(']');
    output
}

fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => {
            serde_json::to_string(&normalize_text(value)).expect("string serialization cannot fail")
        }
        serde_json::Value::Array(values) => {
            let mut output = String::from("[");
            for (idx, value) in values.iter().enumerate() {
                if idx > 0 {
                    output.push(',');
                }
                output.push_str(&canonical_json(value));
            }
            output.push(']');
            output
        }
        serde_json::Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let mut output = String::from("{");
            for (idx, key) in keys.into_iter().enumerate() {
                if idx > 0 {
                    output.push(',');
                }
                output
                    .push_str(&serde_json::to_string(key).expect("key serialization cannot fail"));
                output.push(':');
                output.push_str(&canonical_json(&map[key]));
            }
            output.push('}');
            output
        }
    }
}

fn normalize_text(value: &str) -> String {
    value.nfkc().collect()
}

fn normalize_search_text(value: &str) -> String {
    normalize_text(value).to_lowercase()
}

fn sha256_hex(value: &str) -> String {
    hex::encode(sha2::Sha256::digest(value.as_bytes()))
}

fn string_field<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

fn string_array_field(value: &serde_json::Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn bool_field(value: &serde_json::Value, field: &str) -> Option<bool> {
    value.get(field).and_then(serde_json::Value::as_bool)
}

fn supplemental_id_array_field(value: &serde_json::Value, field: &str) -> Vec<SupplementalId> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(SupplementalId::new)
                .collect()
        })
        .unwrap_or_default()
}

fn sort_records_observation_order(records: &mut [SupplementalRecord]) {
    records.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
}

fn audit_event(
    record: &SupplementalRecord,
    target_claim_id: Option<SupplementalId>,
    code: &str,
    message: &str,
) -> ProjectionAuditEvent {
    ProjectionAuditEvent {
        record_id: record.id.clone(),
        target_claim_id,
        code: code.to_owned(),
        message: message.to_owned(),
        created_at: record.created_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use lethe_core::domain::{
        ActorRef, Mutability, ObservationId, SupplementalId, supplemental::InputAnchorSet,
    };

    fn at(second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 5, 0, 0, second)
            .single()
            .unwrap()
    }

    fn anchors(observation: &str) -> InputAnchorSet {
        InputAnchorSet {
            observations: vec![ObservationId::new(observation)],
            blobs: vec![],
            supplementals: vec![],
        }
    }

    fn claim(
        id: &str,
        observation: &str,
        statement: &str,
        verification_mode: &str,
        created_at: DateTime<Utc>,
        model_version: &str,
    ) -> SupplementalRecord {
        SupplementalRecord {
            id: SupplementalId::new(id),
            kind: CLAIM_KIND.to_owned(),
            derived_from: anchors(observation),
            payload: serde_json::json!({
                "statement": statement,
                "verification_mode": verification_mode
            }),
            created_by: ActorRef::new("actor:test"),
            created_at,
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: Some(model_version.to_owned()),
            consent_metadata: None,
            lineage: None,
        }
    }

    fn transition(
        id: &str,
        target: &str,
        to_state: &str,
        created_at: DateTime<Utc>,
    ) -> SupplementalRecord {
        SupplementalRecord {
            id: SupplementalId::new(id),
            kind: CLAIM_TRANSITION_KIND.to_owned(),
            derived_from: InputAnchorSet {
                observations: vec![],
                blobs: vec![],
                supplementals: vec![SupplementalId::new(target)],
            },
            payload: serde_json::json!({ "to_state": to_state }),
            created_by: ActorRef::new("actor:test"),
            created_at,
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        }
    }

    fn verification(
        id: &str,
        target: &str,
        verdict: &str,
        created_at: DateTime<Utc>,
    ) -> SupplementalRecord {
        SupplementalRecord {
            id: SupplementalId::new(id),
            kind: VERIFICATION_RESULT_KIND.to_owned(),
            derived_from: InputAnchorSet {
                observations: vec![],
                blobs: vec![],
                supplementals: vec![SupplementalId::new(target)],
            },
            payload: serde_json::json!({
                "verdict": verdict,
                "reasoning": "fixture"
            }),
            created_by: ActorRef::new("actor:test"),
            created_at,
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        }
    }

    fn decision(
        id: &str,
        observation: &str,
        statement: &str,
        rationale: &str,
        supersedes: Vec<&str>,
        created_at: DateTime<Utc>,
    ) -> SupplementalRecord {
        SupplementalRecord {
            id: SupplementalId::new(id),
            kind: DECISION_KIND.to_owned(),
            derived_from: anchors(observation),
            payload: serde_json::json!({
                "statement": statement,
                "rationale": rationale,
                "supersedes": supersedes
            }),
            created_by: ActorRef::new("actor:test"),
            created_at,
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        }
    }

    #[test]
    fn batch_rerun_claims_deduplicate_and_keep_absorbed_ids() {
        let records = vec![
            claim(
                "sup:first",
                "obs:conversation",
                "A is true",
                "check",
                at(1),
                "old",
            ),
            claim(
                "sup:rerun",
                "obs:conversation",
                "A is true",
                "check",
                at(2),
                "new",
            ),
        ];

        let projection = ClaimQueueProjector.project_records(&records);

        assert_eq!(projection.claims.len(), 1);
        assert_eq!(projection.claims[0].representative_id.as_str(), "sup:first");
        assert_eq!(
            projection.claims[0]
                .absorbed_ids
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>(),
            vec!["sup:rerun"]
        );
    }

    #[test]
    fn replay_is_deterministic_for_different_input_orders() {
        let records = vec![
            claim("sup:claim", "obs:one", "A is true", "check", at(1), "m1"),
            transition("sup:transition", "sup:claim", "dispatched", at(2)),
            verification("sup:verify", "sup:claim", "consistent", at(3)),
        ];
        let mut reversed = records.clone();
        reversed.reverse();

        let projection_a = ClaimQueueProjector.project_records(&records);
        let projection_b = ClaimQueueProjector.project_records(&reversed);

        assert_eq!(
            serde_json::to_value(&projection_a).unwrap(),
            serde_json::to_value(&projection_b).unwrap()
        );
        assert_eq!(projection_a.claims[0].state, ClaimState::Verified);
    }

    #[test]
    fn invalid_transition_is_skipped_and_audited() {
        let records = vec![
            claim("sup:claim", "obs:one", "A is true", "check", at(1), "m1"),
            verification("sup:verify", "sup:claim", "consistent", at(2)),
            transition("sup:bad", "sup:claim", "dispatched", at(3)),
        ];

        let projection = ClaimQueueProjector.project_records(&records);

        assert_eq!(projection.claims[0].state, ClaimState::Verified);
        assert!(projection.audit_log.iter().any(|event| {
            event.code == "invalid_transition" && event.record_id.as_str() == "sup:bad"
        }));
    }

    #[test]
    fn same_conversation_claims_are_returned_as_one_group() {
        let records = vec![
            claim("sup:a", "obs:conversation", "A", "check", at(1), "m1"),
            claim("sup:b", "obs:conversation", "B", "generate", at(2), "m1"),
            claim("sup:c", "obs:conversation", "C", "check", at(3), "m1"),
        ];

        let projection = ClaimQueueProjector.project_records(&records);

        assert_eq!(projection.groups.len(), 1);
        assert_eq!(projection.groups[0].members.len(), 3);
    }

    #[test]
    fn backfill_filter_is_orthogonal_to_state_filter() {
        let mut backfill = claim(
            "sup:backfill",
            "obs:backfill",
            "Backfill claim",
            "check",
            at(1),
            "m1",
        );
        backfill.payload["backfill"] = serde_json::Value::Bool(true);
        let live = claim("sup:live", "obs:live", "Live claim", "check", at(2), "m1");
        let records = vec![backfill, live];

        let projection = ClaimQueueProjector.project_records(&records);
        let backfill_groups = projection.groups_matching(Some(ClaimState::Open), Some(true));
        let live_groups = projection.groups_matching(Some(ClaimState::Open), Some(false));

        assert_eq!(backfill_groups.len(), 1);
        assert_eq!(
            backfill_groups[0].members[0].representative_id.as_str(),
            "sup:backfill"
        );
        assert_eq!(live_groups.len(), 1);
        assert_eq!(
            live_groups[0].members[0].representative_id.as_str(),
            "sup:live"
        );
    }

    #[test]
    fn decision_supersedes_chain_sets_superseded_by_on_old_decision() {
        let records = vec![
            decision(
                "sup:decision-a",
                "obs:conversation",
                "Use adapter A",
                "old rationale",
                vec![],
                at(1),
            ),
            decision(
                "sup:decision-b",
                "obs:conversation",
                "Use adapter B",
                "replaces adapter A",
                vec!["sup:decision-a"],
                at(2),
            ),
        ];

        let projection = ClaimQueueProjector.project_records(&records);
        let matches = projection.search_decisions("adapter A", 10);
        let old = matches
            .iter()
            .find(|decision| decision.id.as_str() == "sup:decision-a")
            .unwrap();

        assert_eq!(
            old.superseded_by.as_ref().map(SupplementalId::as_str),
            Some("sup:decision-b")
        );
    }
}
