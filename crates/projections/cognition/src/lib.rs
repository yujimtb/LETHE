use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use lethe_core::domain::{
    ConsentDecision, Observation, ObservationId, ProjectionRef, RetractionTarget, SupplementalId,
    SupplementalRecord, consent_decision_from_observation, consent_decision_order,
    observation_privacy_keys,
};
use lethe_engine::projection::runner::Projector;
use lethe_projection_claim_queue::{
    ClaimQueueProjector, ClaimState, DecisionView, ProjectionAuditEvent,
};
use serde::{Deserialize, Serialize};

pub const FRESHNESS_PROJECTION_ID: &str = "proj:freshness";
pub const RESUME_SNAPSHOT_PROJECTION_ID: &str = "proj:resume-snapshot";
pub const PLAN_STATE_PROJECTION_ID: &str = "proj:plan-state";
pub const CARD_QUEUE_PROJECTION_ID: &str = "proj:card-queue";
pub const REPLY_SLO_PROJECTION_ID: &str = "proj:reply-slo";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessThreshold {
    pub source_id: String,
    pub max_age_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessStatus {
    Fresh,
    Missing,
    Unobserved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFreshness {
    pub source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_published: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_recorded_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_observed_at: Option<DateTime<Utc>>,
    pub max_age_seconds: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<i64>,
    pub status: FreshnessStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FreshnessProjection {
    pub sources: Vec<SourceFreshness>,
    pub missing: Vec<SourceFreshness>,
}

#[derive(Debug, Clone)]
pub struct FreshnessProjector {
    thresholds: Vec<FreshnessThreshold>,
    now: DateTime<Utc>,
}

impl FreshnessProjector {
    pub fn new(thresholds: Vec<FreshnessThreshold>, now: DateTime<Utc>) -> Self {
        Self { thresholds, now }
    }

    pub fn project_observations(&self, observations: &[Observation]) -> FreshnessProjection {
        let mut latest: BTreeMap<String, (DateTime<Utc>, DateTime<Utc>)> = BTreeMap::new();
        for observation in observations {
            let source_id = source_id_for_observation(observation);
            latest
                .entry(source_id)
                .and_modify(|(published, recorded_at)| {
                    if observation.published > *published {
                        *published = observation.published;
                    }
                    if observation.recorded_at > *recorded_at {
                        *recorded_at = observation.recorded_at;
                    }
                })
                .or_insert((observation.published, observation.recorded_at));
        }

        let mut configured = self
            .thresholds
            .iter()
            .map(|threshold| {
                let (latest_published, latest_recorded_at, last_observed_at, age_seconds, status) =
                    match latest.get(&threshold.source_id) {
                        Some((published, recorded_at)) => {
                            let last_observed = (*published).max(*recorded_at);
                            let age = (self.now - last_observed).num_seconds();
                            let status = if age > threshold.max_age_seconds {
                                FreshnessStatus::Missing
                            } else {
                                FreshnessStatus::Fresh
                            };
                            (
                                Some(*published),
                                Some(*recorded_at),
                                Some(last_observed),
                                Some(age),
                                status,
                            )
                        }
                        None => (None, None, None, None, FreshnessStatus::Unobserved),
                    };
                SourceFreshness {
                    source_id: threshold.source_id.clone(),
                    latest_published,
                    latest_recorded_at,
                    last_observed_at,
                    max_age_seconds: threshold.max_age_seconds,
                    age_seconds,
                    status,
                }
            })
            .collect::<Vec<_>>();

        configured.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        let missing = configured
            .iter()
            .filter(|source| {
                matches!(
                    source.status,
                    FreshnessStatus::Missing | FreshnessStatus::Unobserved
                )
            })
            .cloned()
            .collect();
        FreshnessProjection {
            sources: configured,
            missing,
        }
    }
}

impl Projector for FreshnessProjector {
    type Input = Observation;
    type Output = FreshnessProjection;

    fn project(&self, inputs: &[Self::Input]) -> Vec<Self::Output> {
        vec![self.project_observations(inputs)]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeProjectCard {
    pub project: String,
    pub last_activity_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_summary: Option<String>,
    pub parkings: Vec<ParkingView>,
    pub open_claims: Vec<OpenClaimView>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResumeSnapshotProjection {
    pub projects: Vec<ResumeProjectCard>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStateProject {
    pub project: String,
    pub open_claim_count: usize,
    pub parking_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_open_claim_age_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_parking_age_seconds: Option<i64>,
    pub decisions: Vec<DecisionView>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanStateProjection {
    pub projects: Vec<PlanStateProject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParkingView {
    pub id: SupplementalId,
    pub statement: String,
    pub resume_context: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenClaimView {
    pub id: SupplementalId,
    pub statement: String,
    pub verification_mode: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CognitionStateProjector {
    now: DateTime<Utc>,
}

impl CognitionStateProjector {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self { now }
    }

    pub fn resume_snapshot(&self, records: &[SupplementalRecord]) -> ResumeSnapshotProjection {
        let claim_queue = ClaimQueueProjector.project_records(records);
        self.resume_snapshot_with_claim_queue(records, &claim_queue)
    }

    pub fn resume_snapshot_with_claim_queue(
        &self,
        records: &[SupplementalRecord],
        claim_queue: &lethe_projection_claim_queue::ClaimQueueProjection,
    ) -> ResumeSnapshotProjection {
        let mut projects: BTreeMap<String, ResumeProjectAccumulator> = BTreeMap::new();
        let mut sorted = records.to_vec();
        sorted.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        for record in &sorted {
            match record.kind.as_str() {
                "session-summary@1" => {
                    let project = project_for_payload(&record.payload);
                    let summary = string_field(&record.payload, "summary").map(str::to_owned);
                    let entry = projects.entry(project.clone()).or_insert_with(|| {
                        ResumeProjectAccumulator::new(project, record.created_at)
                    });
                    entry.touch(record.created_at);
                    if summary.is_some() {
                        entry.session_summary = summary;
                    }
                }
                "parking@1" => {
                    let Some(statement) = string_field(&record.payload, "statement") else {
                        continue;
                    };
                    let Some(resume_context) = string_field(&record.payload, "resume_context")
                    else {
                        continue;
                    };
                    let project = project_for_payload(&record.payload);
                    let entry = projects.entry(project.clone()).or_insert_with(|| {
                        ResumeProjectAccumulator::new(project, record.created_at)
                    });
                    entry.touch(record.created_at);
                    entry.parkings.push(ParkingView {
                        id: record.id.clone(),
                        statement: statement.to_owned(),
                        resume_context: resume_context.to_owned(),
                        created_at: record.created_at,
                    });
                }
                _ => {}
            }
        }
        for claim in claim_queue
            .claims
            .iter()
            .filter(|claim| claim.state == ClaimState::Open)
        {
            let project = claim.project.clone();
            let entry = projects
                .entry(project.clone())
                .or_insert_with(|| ResumeProjectAccumulator::new(project, claim.created_at));
            entry.touch(claim.updated_at);
            entry.open_claims.push(OpenClaimView {
                id: claim.representative_id.clone(),
                statement: claim.statement.clone(),
                verification_mode: claim.verification_mode.clone(),
                created_at: claim.created_at,
            });
        }
        ResumeSnapshotProjection {
            projects: projects
                .into_values()
                .map(ResumeProjectAccumulator::finish)
                .collect(),
        }
    }

    pub fn plan_state(&self, records: &[SupplementalRecord]) -> PlanStateProjection {
        let claim_queue = ClaimQueueProjector.project_records(records);
        let resume = self.resume_snapshot_with_claim_queue(records, &claim_queue);
        self.plan_state_with_claim_queue(resume, &claim_queue)
    }

    pub fn project_with_claim_queue(
        &self,
        records: &[SupplementalRecord],
        claim_queue: &lethe_projection_claim_queue::ClaimQueueProjection,
    ) -> (ResumeSnapshotProjection, PlanStateProjection) {
        let resume = self.resume_snapshot_with_claim_queue(records, claim_queue);
        let plan = self.plan_state_with_claim_queue(resume.clone(), claim_queue);
        (resume, plan)
    }

    fn plan_state_with_claim_queue(
        &self,
        resume: ResumeSnapshotProjection,
        claim_queue: &lethe_projection_claim_queue::ClaimQueueProjection,
    ) -> PlanStateProjection {
        let decisions = current_decisions_by_project(&claim_queue.decisions);
        let mut projects = resume
            .projects
            .into_iter()
            .map(|project| {
                let oldest_claim = project
                    .open_claims
                    .iter()
                    .map(|claim| claim.created_at)
                    .min();
                let oldest_parking = project
                    .parkings
                    .iter()
                    .map(|parking| parking.created_at)
                    .min();
                PlanStateProject {
                    project: project.project.clone(),
                    open_claim_count: project.open_claims.len(),
                    parking_count: project.parkings.len(),
                    oldest_open_claim_age_seconds: oldest_claim
                        .map(|created_at| (self.now - created_at).num_seconds()),
                    oldest_parking_age_seconds: oldest_parking
                        .map(|created_at| (self.now - created_at).num_seconds()),
                    decisions: decisions.get(&project.project).cloned().unwrap_or_default(),
                }
            })
            .collect::<Vec<_>>();
        for (project, project_decisions) in decisions {
            if !projects.iter().any(|entry| entry.project == project) {
                projects.push(PlanStateProject {
                    project,
                    open_claim_count: 0,
                    parking_count: 0,
                    oldest_open_claim_age_seconds: None,
                    oldest_parking_age_seconds: None,
                    decisions: project_decisions,
                });
            }
        }
        projects.sort_by(|left, right| left.project.cmp(&right.project));
        PlanStateProjection { projects }
    }
}

#[derive(Debug, Clone)]
struct ResumeProjectAccumulator {
    project: String,
    last_activity_at: DateTime<Utc>,
    session_summary: Option<String>,
    parkings: Vec<ParkingView>,
    open_claims: Vec<OpenClaimView>,
}

impl ResumeProjectAccumulator {
    fn new(project: String, last_activity_at: DateTime<Utc>) -> Self {
        Self {
            project,
            last_activity_at,
            session_summary: None,
            parkings: Vec::new(),
            open_claims: Vec::new(),
        }
    }

    fn touch(&mut self, at: DateTime<Utc>) {
        self.last_activity_at = self.last_activity_at.max(at);
    }

    fn finish(mut self) -> ResumeProjectCard {
        self.parkings
            .sort_by(|left, right| left.created_at.cmp(&right.created_at));
        self.open_claims
            .sort_by(|left, right| left.created_at.cmp(&right.created_at));
        ResumeProjectCard {
            project: self.project,
            last_activity_at: self.last_activity_at,
            session_summary: self.session_summary,
            parkings: self.parkings,
            open_claims: self.open_claims,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardState {
    Pending,
    Approved,
    Sent,
    Skipped,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyCard {
    pub draft_id: SupplementalId,
    pub agent_name: Option<String>,
    pub channel: String,
    pub recipient: String,
    pub body: String,
    pub state: CardState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<SupplementalId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_interface: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_record_id: Option<SupplementalId>,
    pub automatic_send: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CardQueueProjection {
    pub cards: Vec<ReplyCard>,
    pub audit_log: Vec<ProjectionAuditEvent>,
}

#[derive(Debug, Clone)]
pub struct CardQueueProjector {
    now: DateTime<Utc>,
}

impl CardQueueProjector {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self { now }
    }

    pub fn project_records(&self, records: &[SupplementalRecord]) -> CardQueueProjection {
        CardQueueReducer::from_records(records).projection(self.now)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CardQueueReducer {
    records: BTreeMap<String, SupplementalRecord>,
    drafts: BTreeMap<String, SupplementalRecord>,
    events_by_draft: BTreeMap<String, BTreeMap<(DateTime<Utc>, String), SupplementalRecord>>,
    cards: BTreeMap<String, ReplyCard>,
    audit_by_event: BTreeMap<(DateTime<Utc>, String), Vec<ProjectionAuditEvent>>,
    expiries: BTreeMap<DateTime<Utc>, BTreeSet<String>>,
}

impl CardQueueReducer {
    pub fn from_records(records: &[SupplementalRecord]) -> Self {
        let mut reducer = Self::default();
        let mut sorted = records
            .iter()
            .filter(|record| is_card_queue_kind(&record.kind))
            .cloned()
            .collect::<Vec<_>>();
        sorted.sort_by(record_order);
        for record in sorted
            .iter()
            .filter(|record| record.kind == "reply-draft@1")
        {
            reducer.upsert_record(record.clone());
        }
        for record in sorted
            .into_iter()
            .filter(|record| record.kind != "reply-draft@1")
        {
            reducer.upsert_record(record);
        }
        reducer
    }

    pub fn upsert_record(&mut self, record: SupplementalRecord) {
        if !is_card_queue_kind(&record.kind) {
            return;
        }
        if self.records.contains_key(record.id.as_str()) {
            self.records.insert(record.id.as_str().to_owned(), record);
            self.rebuild();
            return;
        }
        self.records
            .insert(record.id.as_str().to_owned(), record.clone());
        match record.kind.as_str() {
            "reply-draft@1" => {
                let draft_id = record.id.as_str().to_owned();
                self.drafts.insert(draft_id.clone(), record);
                self.recompute_card(&draft_id);
            }
            "reply-approval@1" | "send-record@1" => {
                let key = record_key(&record);
                let Some(draft_id) = single_draft_anchor(&record) else {
                    self.audit_by_event.insert(
                        key,
                        vec![audit_event(
                            &record,
                            "missing_draft_anchor",
                            if record.kind == "reply-approval@1" {
                                "approval has no draft anchor"
                            } else {
                                "send record has no draft anchor"
                            },
                        )],
                    );
                    return;
                };
                let draft_id = draft_id.as_str().to_owned();
                self.events_by_draft
                    .entry(draft_id.clone())
                    .or_default()
                    .insert(key, record);
                self.recompute_card(&draft_id);
            }
            _ => unreachable!("card queue kind routing is exhaustive"),
        }
    }

    pub fn remove_record(&mut self, id: &SupplementalId) {
        if self.records.remove(id.as_str()).is_some() {
            self.rebuild();
        }
    }

    pub fn draft(&self, id: &SupplementalId) -> Option<&SupplementalRecord> {
        self.drafts.get(id.as_str())
    }

    pub fn projection(&self, now: DateTime<Utc>) -> CardQueueProjection {
        let mut cards = self.cards.clone();
        for (_, draft_ids) in self.expiries.range(..now) {
            for draft_id in draft_ids {
                if let Some(card) = cards.get_mut(draft_id)
                    && card.state == CardState::Pending
                {
                    card.state = CardState::Expired;
                    card.updated_at = now;
                }
            }
        }
        CardQueueProjection {
            cards: cards.into_values().collect(),
            audit_log: self
                .audit_by_event
                .values()
                .flat_map(|events| events.iter().cloned())
                .collect(),
        }
    }

    fn rebuild(&mut self) {
        let records = self.records.values().cloned().collect::<Vec<_>>();
        *self = Self::from_records(&records);
    }

    fn recompute_card(&mut self, draft_id: &str) {
        for draft_ids in self.expiries.values_mut() {
            draft_ids.remove(draft_id);
        }
        self.expiries.retain(|_, draft_ids| !draft_ids.is_empty());
        if let Some(events) = self.events_by_draft.get(draft_id) {
            for key in events.keys() {
                self.audit_by_event.remove(key);
            }
        }

        let Some(draft) = self.drafts.get(draft_id) else {
            if let Some(events) = self.events_by_draft.get(draft_id) {
                for (key, event) in events {
                    self.audit_by_event.insert(
                        key.clone(),
                        vec![audit_event(
                            event,
                            "unknown_draft_anchor",
                            if event.kind == "reply-approval@1" {
                                "approval anchors unknown draft"
                            } else {
                                "send record anchors unknown draft"
                            },
                        )],
                    );
                }
            }
            self.cards.remove(draft_id);
            return;
        };
        let Some(channel) = string_field(&draft.payload, "channel") else {
            self.mark_events_unknown(draft_id);
            self.cards.remove(draft_id);
            return;
        };
        let Some(recipient) = string_field(&draft.payload, "recipient") else {
            self.mark_events_unknown(draft_id);
            self.cards.remove(draft_id);
            return;
        };
        let Some(body) = string_field(&draft.payload, "body") else {
            self.mark_events_unknown(draft_id);
            self.cards.remove(draft_id);
            return;
        };
        let mut cards = BTreeMap::from([(
            draft_id.to_owned(),
            ReplyCard {
                draft_id: draft.id.clone(),
                agent_name: derive_agent_name(draft),
                channel: channel.to_owned(),
                recipient: recipient.to_owned(),
                body: body.to_owned(),
                state: CardState::Pending,
                created_at: draft.created_at,
                updated_at: draft.created_at,
                approval_id: None,
                approval_interface: None,
                sent_record_id: None,
                automatic_send: false,
            },
        )]);
        if let Some(events) = self.events_by_draft.get(draft_id) {
            for (key, event) in events {
                let mut audit = Vec::new();
                match event.kind.as_str() {
                    "reply-approval@1" => apply_approval(event, &mut cards, &mut audit),
                    "send-record@1" => apply_send(event, &mut cards, &mut audit),
                    _ => unreachable!("card event routing is exhaustive"),
                }
                if !audit.is_empty() {
                    self.audit_by_event.insert(key.clone(), audit);
                }
            }
        }
        let card = cards
            .remove(draft_id)
            .expect("replayed draft card must remain present");
        if card.state == CardState::Pending
            && let Some(expires_at) = draft_expiry(draft)
        {
            self.expiries
                .entry(expires_at)
                .or_default()
                .insert(draft_id.to_owned());
        }
        self.cards.insert(draft_id.to_owned(), card);
    }

    fn mark_events_unknown(&mut self, draft_id: &str) {
        if let Some(events) = self.events_by_draft.get(draft_id) {
            for (key, event) in events {
                self.audit_by_event.insert(
                    key.clone(),
                    vec![audit_event(
                        event,
                        "unknown_draft_anchor",
                        if event.kind == "reply-approval@1" {
                            "approval anchors unknown draft"
                        } else {
                            "send record anchors unknown draft"
                        },
                    )],
                );
            }
        }
    }
}

impl Projector for CardQueueProjector {
    type Input = SupplementalRecord;
    type Output = CardQueueProjection;

    fn project(&self, inputs: &[Self::Input]) -> Vec<Self::Output> {
        vec![self.project_records(inputs)]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplySloStatus {
    Pending,
    Overdue,
    SentOnTime,
    SentLate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyLatency {
    pub incoming_observation_id: ObservationId,
    pub channel_id: String,
    pub sender_id: String,
    pub thread_ref: String,
    pub published: DateTime<Utc>,
    pub due_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_seconds: Option<i64>,
    pub status: ReplySloStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplySloProjection {
    pub rows: Vec<ReplyLatency>,
    pub overdue: Vec<ReplyLatency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CommunicationThreadKey {
    pub channel_id: String,
    pub thread_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommunicationFact {
    pub incoming_observation_id: ObservationId,
    pub channel_id: String,
    pub sender_id: String,
    pub thread_ref: String,
    pub published: DateTime<Utc>,
    pub due_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommunicationProjectionState {
    facts_by_thread: BTreeMap<String, BTreeMap<String, CommunicationFact>>,
    observation_keys: BTreeMap<String, CommunicationThreadKey>,
    #[serde(skip)]
    observations: BTreeMap<String, Observation>,
    #[serde(default)]
    source_object_ids: BTreeMap<String, String>,
    #[serde(default)]
    observation_subjects: BTreeMap<String, String>,
    #[serde(default)]
    retracted_observation_ids: BTreeSet<String>,
    #[serde(default)]
    retracted_source_object_ids: BTreeSet<String>,
    #[serde(default)]
    consent_by_subject: BTreeMap<String, ConsentDecision>,
    #[serde(default)]
    consent_by_identifier: BTreeMap<String, ConsentDecision>,
    #[serde(default)]
    observation_ids_by_privacy_key: BTreeMap<String, BTreeSet<String>>,
}

impl CommunicationProjectionState {
    pub fn from_observations(observations: &[Observation], join_index: &ReplySloJoinIndex) -> Self {
        let mut state = Self::default();
        state.fold_observations(observations, join_index);
        state
    }

    pub fn fold_observations(
        &mut self,
        observations: &[Observation],
        join_index: &ReplySloJoinIndex,
    ) -> ReplySloProjection {
        let mut delta = Vec::new();
        for observation in observations {
            if observation.schema.as_str() == "schema:consent-decision" {
                for privacy_key in self.apply_consent(observation) {
                    self.rematerialize_privacy_key(&privacy_key, join_index, &mut delta);
                }
                continue;
            }
            if let Some(value) = observation.meta.get("retracts") {
                let Ok(target) = RetractionTarget::from_value(value) else {
                    continue;
                };
                if let Some(observation_id) = target.observation_id {
                    self.retracted_observation_ids
                        .insert(observation_id.as_str().to_owned());
                    self.remove_observation(observation_id.as_str());
                }
                if let Some(source_object_id) = target.source_object_id {
                    self.retracted_source_object_ids
                        .insert(source_object_id.clone());
                    let target_ids = self
                        .source_object_ids
                        .iter()
                        .filter(|(_, object_id)| *object_id == &source_object_id)
                        .map(|(id, _)| id.clone())
                        .collect::<Vec<_>>();
                    for observation_id in target_ids {
                        self.remove_observation(&observation_id);
                    }
                }
                continue;
            }
            if communication_fact(observation, join_index).is_some() {
                self.remember_observation(observation);
            }
            self.materialize_observation(observation, join_index, &mut delta);
        }
        sort_reply_slo_rows(&mut delta);
        let overdue = delta
            .iter()
            .filter(|row| is_overdue_reply(row.status))
            .cloned()
            .collect();
        ReplySloProjection {
            rows: delta,
            overdue,
        }
    }

    /// Remember canonical observations without materializing them.  This is
    /// used by the consumer lane when a persisted state receives a
    /// re-consent and must restore previously shielded communication facts.
    pub fn remember_observations(
        &mut self,
        observations: &[Observation],
        join_index: &ReplySloJoinIndex,
    ) {
        for observation in observations {
            if observation.schema.as_str() == "schema:consent-decision"
                || observation.meta.get("retracts").is_some()
                || communication_fact(observation, join_index).is_none()
            {
                continue;
            }
            self.remember_observation(observation);
        }
    }

    fn remember_observation(&mut self, observation: &Observation) {
        let observation_id = observation.id.as_str().to_owned();
        self.observations
            .insert(observation_id.clone(), observation.clone());
        self.observation_subjects.insert(
            observation_id.clone(),
            observation.subject.as_str().to_owned(),
        );
        if let Some(source_object_id) = observation
            .meta
            .get("object_id")
            .and_then(serde_json::Value::as_str)
        {
            self.source_object_ids
                .insert(observation_id.clone(), source_object_id.to_owned());
        }
        for privacy_key in observation_privacy_keys(observation) {
            self.observation_ids_by_privacy_key
                .entry(privacy_key)
                .or_default()
                .insert(observation_id.clone());
        }
    }

    fn apply_consent(&mut self, observation: &Observation) -> BTreeSet<String> {
        let Some(decision) = consent_decision_from_observation(observation) else {
            return BTreeSet::new();
        };
        let mut changed = BTreeSet::new();
        if update_latest_consent(
            &mut self.consent_by_subject,
            decision.subject.clone(),
            decision.clone(),
        ) {
            changed.insert(decision.subject.clone());
        }
        if let Some(identifier) = decision.identifier.clone()
            && update_latest_consent(
                &mut self.consent_by_identifier,
                identifier.clone(),
                decision,
            )
        {
            changed.insert(identifier);
        }
        changed
    }

    fn materialize_observation(
        &mut self,
        observation: &Observation,
        join_index: &ReplySloJoinIndex,
        delta: &mut Vec<ReplyLatency>,
    ) {
        let Some(fact) = communication_fact(observation, join_index) else {
            return;
        };
        let observation_id = fact.incoming_observation_id.as_str().to_owned();
        self.remove_observation(&observation_id);
        if !self.visible(observation) {
            return;
        }
        let key = CommunicationThreadKey {
            channel_id: fact.channel_id.clone(),
            thread_ref: fact.thread_ref.clone(),
        };
        let storage_key = communication_thread_storage_key(&key);
        self.observation_keys.insert(observation_id.clone(), key);
        self.facts_by_thread
            .entry(storage_key)
            .or_default()
            .insert(observation_id, fact.clone());
        delta.push(reply_latency_from_fact(&fact, Utc::now()));
    }

    fn rematerialize_privacy_key(
        &mut self,
        privacy_key: &str,
        join_index: &ReplySloJoinIndex,
        delta: &mut Vec<ReplyLatency>,
    ) {
        let observation_ids = self
            .observation_ids_by_privacy_key
            .get(privacy_key)
            .cloned()
            .unwrap_or_default();

        // Canonical observations are deliberately not serialized into this
        // materialization. Remove persisted facts first so a restored state
        // cannot retain an opted-out record while the observation is being
        // re-pulled from storage.
        for observation_id in &observation_ids {
            self.remove_observation(observation_id);
        }
        for observation_id in observation_ids {
            let Some(observation) = self.observations.get(&observation_id).cloned() else {
                continue;
            };
            self.materialize_observation(&observation, join_index, delta);
        }
    }

    fn visible(&self, observation: &Observation) -> bool {
        if self
            .retracted_observation_ids
            .contains(observation.id.as_str())
        {
            return false;
        }
        if observation
            .meta
            .get("object_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| self.retracted_source_object_ids.contains(id))
        {
            return false;
        }
        let identifiers = observation_privacy_keys(observation)
            .into_iter()
            .filter(|key| key != observation.subject.as_str())
            .collect::<Vec<_>>();
        self.consent_by_subject
            .get(observation.subject.as_str())
            .into_iter()
            .chain(
                identifiers
                    .iter()
                    .filter_map(|identifier| self.consent_by_identifier.get(identifier)),
            )
            .max_by(|left, right| {
                consent_decision_order(
                    left.published,
                    left.recorded_at,
                    left.observation_id.as_str(),
                )
                .cmp(&consent_decision_order(
                    right.published,
                    right.recorded_at,
                    right.observation_id.as_str(),
                ))
            })
            .is_none_or(|decision| decision.status != "opted_out")
    }

    fn remove_observation(&mut self, observation_id: &str) {
        if let Some(key) = self.observation_keys.remove(observation_id)
            && let Some(facts) = self
                .facts_by_thread
                .get_mut(&communication_thread_storage_key(&key))
        {
            facts.remove(observation_id);
            if facts.is_empty() {
                self.facts_by_thread
                    .remove(&communication_thread_storage_key(&key));
            }
        }
    }

    pub fn refresh_sent_at(
        &mut self,
        observation_id: &ObservationId,
        sent_at: Option<DateTime<Utc>>,
    ) {
        let Some(key) = self.observation_keys.get(observation_id.as_str()) else {
            return;
        };
        if let Some(fact) = self
            .facts_by_thread
            .get_mut(&communication_thread_storage_key(key))
            .and_then(|facts| facts.get_mut(observation_id.as_str()))
        {
            fact.sent_at = sent_at;
        }
    }

    pub fn project(&self, now: DateTime<Utc>) -> ReplySloProjection {
        let mut rows = self
            .facts_by_thread
            .values()
            .flat_map(|facts| facts.values())
            .map(|fact| reply_latency_from_fact(fact, now))
            .collect::<Vec<_>>();
        sort_reply_slo_rows(&mut rows);
        let overdue = rows
            .iter()
            .filter(|row| is_overdue_reply(row.status))
            .cloned()
            .collect();
        ReplySloProjection { rows, overdue }
    }

    pub fn project_observations(
        &self,
        observations: &[Observation],
        now: DateTime<Utc>,
    ) -> ReplySloProjection {
        let mut rows = observations
            .iter()
            .filter_map(|observation| {
                let key = self.observation_keys.get(observation.id.as_str())?;
                self.facts_by_thread
                    .get(&communication_thread_storage_key(key))?
                    .get(observation.id.as_str())
                    .map(|fact| reply_latency_from_fact(fact, now))
            })
            .collect::<Vec<_>>();
        sort_reply_slo_rows(&mut rows);
        let overdue = rows
            .iter()
            .filter(|row| is_overdue_reply(row.status))
            .cloned()
            .collect();
        ReplySloProjection { rows, overdue }
    }

    pub fn len(&self) -> usize {
        self.observation_keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.observation_keys.is_empty()
    }
}

fn update_latest_consent(
    index: &mut BTreeMap<String, ConsentDecision>,
    key: String,
    decision: ConsentDecision,
) -> bool {
    match index.get(&key) {
        Some(current)
            if consent_decision_order(
                current.published,
                current.recorded_at,
                current.observation_id.as_str(),
            ) >= consent_decision_order(
                decision.published,
                decision.recorded_at,
                decision.observation_id.as_str(),
            ) =>
        {
            false
        }
        _ => {
            index.insert(key, decision);
            true
        }
    }
}

fn communication_thread_storage_key(key: &CommunicationThreadKey) -> String {
    serde_json::to_string(&(key.channel_id.as_str(), key.thread_ref.as_str()))
        .expect("communication thread key serialization must not fail")
}

fn communication_fact(
    observation: &Observation,
    join_index: &ReplySloJoinIndex,
) -> Option<CommunicationFact> {
    let channel_id = communication_meta(observation, "communication_channel_id")?;
    let sender_id = communication_meta(observation, "communication_sender_id")?;
    let thread_ref = communication_meta(observation, "communication_thread_ref")?;
    let due_at = observation
        .meta
        .pointer("/communication/reply_due_at")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_datetime)?;
    Some(CommunicationFact {
        incoming_observation_id: observation.id.clone(),
        channel_id: channel_id.to_owned(),
        sender_id: sender_id.to_owned(),
        thread_ref: thread_ref.to_owned(),
        published: observation.published,
        due_at,
        sent_at: join_index.sent_at_for_observation(&observation.id),
    })
}

fn reply_latency_from_fact(fact: &CommunicationFact, now: DateTime<Utc>) -> ReplyLatency {
    let latency_seconds = fact
        .sent_at
        .map(|sent_at| (sent_at - fact.published).num_seconds());
    let status = match fact.sent_at {
        Some(sent_at) if sent_at <= fact.due_at => ReplySloStatus::SentOnTime,
        Some(_) => ReplySloStatus::SentLate,
        None if now > fact.due_at => ReplySloStatus::Overdue,
        None => ReplySloStatus::Pending,
    };
    ReplyLatency {
        incoming_observation_id: fact.incoming_observation_id.clone(),
        channel_id: fact.channel_id.clone(),
        sender_id: fact.sender_id.clone(),
        thread_ref: fact.thread_ref.clone(),
        published: fact.published,
        due_at: fact.due_at,
        sent_at: fact.sent_at,
        latency_seconds,
        status,
    }
}

fn sort_reply_slo_rows(rows: &mut [ReplyLatency]) {
    rows.sort_by(|left, right| {
        left.due_at.cmp(&right.due_at).then_with(|| {
            left.incoming_observation_id
                .as_str()
                .cmp(right.incoming_observation_id.as_str())
        })
    });
}

fn is_overdue_reply(status: ReplySloStatus) -> bool {
    matches!(status, ReplySloStatus::Overdue | ReplySloStatus::SentLate)
}

#[derive(Debug, Clone)]
pub struct ReplySloProjector {
    now: DateTime<Utc>,
}

impl ReplySloProjector {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self { now }
    }

    pub fn project_records(
        &self,
        observations: &[Observation],
        records: &[SupplementalRecord],
    ) -> ReplySloProjection {
        let join_index = ReplySloJoinIndex::from_records(records);
        self.project_observations(observations, &join_index)
    }

    pub fn project_observations(
        &self,
        observations: &[Observation],
        join_index: &ReplySloJoinIndex,
    ) -> ReplySloProjection {
        let mut rows = observations
            .iter()
            .filter_map(|observation| {
                let channel_id = communication_meta(observation, "communication_channel_id")?;
                let sender_id = communication_meta(observation, "communication_sender_id")?;
                let thread_ref = communication_meta(observation, "communication_thread_ref")?;
                let due_at = observation
                    .meta
                    .pointer("/communication/reply_due_at")
                    .and_then(serde_json::Value::as_str)
                    .and_then(parse_datetime)?;
                let sent_at = join_index
                    .sent_by_observation
                    .get(observation.id.as_str())
                    .copied();
                let latency_seconds =
                    sent_at.map(|sent_at| (sent_at - observation.published).num_seconds());
                let status = match sent_at {
                    Some(sent_at) if sent_at <= due_at => ReplySloStatus::SentOnTime,
                    Some(_) => ReplySloStatus::SentLate,
                    None if self.now > due_at => ReplySloStatus::Overdue,
                    None => ReplySloStatus::Pending,
                };
                Some(ReplyLatency {
                    incoming_observation_id: observation.id.clone(),
                    channel_id: channel_id.to_owned(),
                    sender_id: sender_id.to_owned(),
                    thread_ref: thread_ref.to_owned(),
                    published: observation.published,
                    due_at,
                    sent_at,
                    latency_seconds,
                    status,
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.due_at.cmp(&right.due_at).then_with(|| {
                left.incoming_observation_id
                    .as_str()
                    .cmp(right.incoming_observation_id.as_str())
            })
        });
        let overdue = rows
            .iter()
            .filter(|row| {
                matches!(
                    row.status,
                    ReplySloStatus::Overdue | ReplySloStatus::SentLate
                )
            })
            .cloned()
            .collect();
        ReplySloProjection { rows, overdue }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplySloJoinIndex {
    records: BTreeMap<String, SupplementalRecord>,
    draft_to_observation: BTreeMap<String, ObservationId>,
    sent_by_observation: BTreeMap<String, DateTime<Utc>>,
}

impl ReplySloJoinIndex {
    pub fn from_records(records: &[SupplementalRecord]) -> Self {
        let mut index = Self::default();
        let mut relevant = records
            .iter()
            .filter(|record| matches!(record.kind.as_str(), "reply-draft@1" | "send-record@1"))
            .cloned()
            .collect::<Vec<_>>();
        relevant.sort_by(record_order);
        for record in relevant
            .iter()
            .filter(|record| record.kind == "reply-draft@1")
        {
            index.upsert_record(record.clone());
        }
        for record in relevant
            .into_iter()
            .filter(|record| record.kind == "send-record@1")
        {
            index.upsert_record(record);
        }
        index
    }

    pub fn upsert_record(&mut self, record: SupplementalRecord) {
        if !matches!(record.kind.as_str(), "reply-draft@1" | "send-record@1") {
            return;
        }
        if self.records.contains_key(record.id.as_str()) {
            self.records.insert(record.id.as_str().to_owned(), record);
            self.rebuild();
            return;
        }
        self.records
            .insert(record.id.as_str().to_owned(), record.clone());
        match record.kind.as_str() {
            "reply-draft@1" => {
                if let Some(observation_id) = record.derived_from.observations.first() {
                    self.draft_to_observation
                        .insert(record.id.as_str().to_owned(), observation_id.clone());
                }
            }
            "send-record@1" => {
                let Some(draft_id) = single_draft_anchor(&record) else {
                    return;
                };
                let Some(observation_id) = self.draft_to_observation.get(draft_id.as_str()) else {
                    return;
                };
                let Some(sent_at) =
                    string_field(&record.payload, "sent_at").and_then(parse_datetime)
                else {
                    return;
                };
                self.sent_by_observation
                    .entry(observation_id.as_str().to_owned())
                    .and_modify(|current| *current = (*current).min(sent_at))
                    .or_insert(sent_at);
            }
            _ => unreachable!("reply SLO kind routing is exhaustive"),
        }
    }

    pub fn sent_at_for_observation(&self, observation_id: &ObservationId) -> Option<DateTime<Utc>> {
        self.sent_by_observation
            .get(observation_id.as_str())
            .copied()
    }

    pub fn observation_id_for_record(&self, record: &SupplementalRecord) -> Option<ObservationId> {
        match record.kind.as_str() {
            "reply-draft@1" => record.derived_from.observations.first().cloned(),
            "send-record@1" => single_draft_anchor(record)
                .and_then(|draft_id| self.draft_to_observation.get(draft_id.as_str()).cloned()),
            _ => None,
        }
    }

    pub fn remove_record(&mut self, id: &SupplementalId) {
        if self.records.remove(id.as_str()).is_some() {
            self.rebuild();
        }
    }

    fn rebuild(&mut self) {
        let records = self.records.values().cloned().collect::<Vec<_>>();
        *self = Self::from_records(&records);
    }
}

pub fn freshness_ref() -> ProjectionRef {
    ProjectionRef::new(FRESHNESS_PROJECTION_ID)
}

pub fn resume_snapshot_ref() -> ProjectionRef {
    ProjectionRef::new(RESUME_SNAPSHOT_PROJECTION_ID)
}

pub fn plan_state_ref() -> ProjectionRef {
    ProjectionRef::new(PLAN_STATE_PROJECTION_ID)
}

pub fn card_queue_ref() -> ProjectionRef {
    ProjectionRef::new(CARD_QUEUE_PROJECTION_ID)
}

pub fn reply_slo_ref() -> ProjectionRef {
    ProjectionRef::new(REPLY_SLO_PROJECTION_ID)
}

fn source_id_for_observation(observation: &Observation) -> String {
    observation
        .meta
        .get("communication_channel_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            observation
                .source_system
                .as_ref()
                .map(|value| value.as_str().to_owned())
        })
        .expect("observation must have communication_channel_id meta or source_system")
}

fn project_for_payload(payload: &serde_json::Value) -> String {
    string_field(payload, "project")
        .filter(|project| !project.trim().is_empty())
        .unwrap_or("uncategorized")
        .to_owned()
}

fn current_decisions_by_project(decisions: &[DecisionView]) -> BTreeMap<String, Vec<DecisionView>> {
    let mut superseded = BTreeSet::new();
    for decision in decisions {
        if decision.superseded_by.is_some() {
            superseded.insert(decision.id.as_str().to_owned());
        }
    }
    let mut by_project = BTreeMap::<String, Vec<DecisionView>>::new();
    for decision in decisions {
        if superseded.contains(decision.id.as_str()) {
            continue;
        }
        by_project
            .entry(decision.project.clone())
            .or_default()
            .push(decision.clone());
    }
    by_project
}

fn apply_approval(
    record: &SupplementalRecord,
    cards: &mut BTreeMap<String, ReplyCard>,
    audit_log: &mut Vec<ProjectionAuditEvent>,
) {
    let Some(draft_id) = single_draft_anchor(record) else {
        audit_log.push(audit_event(
            record,
            "missing_draft_anchor",
            "approval has no draft anchor",
        ));
        return;
    };
    let Some(card) = cards.get_mut(draft_id.as_str()) else {
        audit_log.push(audit_event(
            record,
            "unknown_draft_anchor",
            "approval anchors unknown draft",
        ));
        return;
    };
    if card.state != CardState::Pending {
        return;
    }
    match string_field(&record.payload, "decision") {
        Some("approved") => {
            card.state = CardState::Approved;
            card.updated_at = record.created_at;
            card.approval_id = Some(record.id.clone());
            card.approval_interface = string_field(&record.payload, "interface").map(str::to_owned);
        }
        Some("skipped") => {
            card.state = CardState::Skipped;
            card.updated_at = record.created_at;
            card.approval_id = Some(record.id.clone());
            card.approval_interface = string_field(&record.payload, "interface").map(str::to_owned);
        }
        Some(other) => audit_log.push(audit_event(
            record,
            "invalid_approval_decision",
            &format!("unsupported approval decision {other}"),
        )),
        None => audit_log.push(audit_event(
            record,
            "malformed_approval",
            "reply-approval@1 missing decision",
        )),
    }
}

fn apply_send(
    record: &SupplementalRecord,
    cards: &mut BTreeMap<String, ReplyCard>,
    audit_log: &mut Vec<ProjectionAuditEvent>,
) {
    let Some(draft_id) = single_draft_anchor(record) else {
        audit_log.push(audit_event(
            record,
            "missing_draft_anchor",
            "send record has no draft anchor",
        ));
        return;
    };
    let Some(card) = cards.get_mut(draft_id.as_str()) else {
        audit_log.push(audit_event(
            record,
            "unknown_draft_anchor",
            "send record anchors unknown draft",
        ));
        return;
    };
    match string_field(&record.payload, "mode") {
        Some("automatic") if matches!(card.state, CardState::Pending | CardState::Approved) => {
            card.state = CardState::Sent;
            card.updated_at = record.created_at;
            card.sent_record_id = Some(record.id.clone());
            card.automatic_send = true;
        }
        Some("approved") if card.state == CardState::Approved => {
            card.state = CardState::Sent;
            card.updated_at = record.created_at;
            card.sent_record_id = Some(record.id.clone());
        }
        Some(mode) => audit_log.push(audit_event(
            record,
            "invalid_send_transition",
            &format!("send mode {mode} is invalid from current card state"),
        )),
        None => audit_log.push(audit_event(
            record,
            "malformed_send_record",
            "send-record@1 missing mode",
        )),
    }
}

fn single_draft_anchor(record: &SupplementalRecord) -> Option<&SupplementalId> {
    record.derived_from.supplementals.first()
}

fn is_card_queue_kind(kind: &str) -> bool {
    matches!(kind, "reply-draft@1" | "reply-approval@1" | "send-record@1")
}

fn record_order(left: &SupplementalRecord, right: &SupplementalRecord) -> std::cmp::Ordering {
    left.created_at
        .cmp(&right.created_at)
        .then_with(|| left.id.as_str().cmp(right.id.as_str()))
}

fn record_key(record: &SupplementalRecord) -> (DateTime<Utc>, String) {
    (record.created_at, record.id.as_str().to_owned())
}

fn draft_expiry(draft: &SupplementalRecord) -> Option<DateTime<Utc>> {
    string_field(&draft.payload, "expires_at").and_then(parse_datetime)
}

fn derive_agent_name(draft: &SupplementalRecord) -> Option<String> {
    agent_name_from_created_by(draft.created_by.as_str()).or_else(|| {
        draft
            .lineage
            .as_ref()
            .and_then(|lineage| agent_name_from_lineage(lineage.as_str()))
    })
}

fn agent_name_from_created_by(created_by: &str) -> Option<String> {
    let name = created_by.strip_prefix("agent:")?;
    (!name.is_empty()).then(|| name.to_owned())
}

fn agent_name_from_lineage(lineage: &str) -> Option<String> {
    let (_, name) = lineage.rsplit_once("/agent/")?;
    if name.is_empty() || name.contains('/') {
        return None;
    }
    Some(name.to_owned())
}

fn audit_event(record: &SupplementalRecord, code: &str, message: &str) -> ProjectionAuditEvent {
    ProjectionAuditEvent {
        record_id: record.id.clone(),
        target_claim_id: None,
        code: code.to_owned(),
        message: message.to_owned(),
        created_at: record.created_at,
    }
}

fn string_field<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(serde_json::Value::as_str)
}

fn communication_meta<'a>(observation: &'a Observation, field: &str) -> Option<&'a str> {
    observation
        .meta
        .get(field)
        .and_then(serde_json::Value::as_str)
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.to_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use lethe_core::domain::supplemental::InputAnchorSet;
    use lethe_core::domain::{
        ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, LineageRef, Mutability,
        ObservationId, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
    };

    fn at(second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 6, 0, 0, second)
            .single()
            .unwrap()
    }

    fn observation(source: &str, published: DateTime<Utc>) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:test"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: Some(SourceSystemRef::new(source)),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("entity:test"),
            target: None,
            payload: serde_json::json!({}),
            attachments: vec![],
            published,
            recorded_at: published,
            consent: None,
            idempotency_key: IdempotencyKey::new(format!("{source}:1")),
            meta: serde_json::json!({}),
        }
    }

    fn communication_observation(
        id: &str,
        channel_id: &str,
        published: DateTime<Utc>,
        due_at: DateTime<Utc>,
    ) -> Observation {
        let mut observation = observation("sys:gmail", published);
        observation.id = ObservationId::new(id);
        observation.meta = serde_json::json!({
            "communication_channel_id": channel_id,
            "communication_sender_id": "sender@example.test",
            "communication_thread_ref": "gmail:thread:thread-1",
            "communication": {
                "reply_due_at": due_at,
            }
        });
        observation
    }

    fn consent_observation(subject: &str, status: &str, published: DateTime<Utc>) -> Observation {
        let mut observation = observation("sys:consent", published);
        observation.id = ObservationId::new(format!("consent:{status}:{}", published.timestamp()));
        observation.subject = EntityRef::new(subject);
        observation.schema = SchemaRef::new("schema:consent-decision");
        observation.payload = serde_json::json!({
            "status": status,
            "identifier": "sender@example.test",
        });
        observation.recorded_at = published + chrono::Duration::seconds(1);
        observation
    }

    fn observation_with_source_instance(
        source: &str,
        source_instance: &str,
        published: DateTime<Utc>,
    ) -> Observation {
        let mut observation = observation(source, published);
        observation.meta = serde_json::json!({
            "source_instance": source_instance
        });
        observation
    }

    fn supplemental(
        id: &str,
        kind: &str,
        payload: serde_json::Value,
        anchors: InputAnchorSet,
        created_at: DateTime<Utc>,
    ) -> SupplementalRecord {
        SupplementalRecord {
            id: SupplementalId::new(id),
            kind: kind.to_owned(),
            derived_from: anchors,
            payload,
            created_by: ActorRef::new("actor:test"),
            created_at,
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        }
    }

    fn obs_anchor(id: &str) -> InputAnchorSet {
        InputAnchorSet {
            observations: vec![ObservationId::new(id)],
            blobs: vec![],
            supplementals: vec![],
        }
    }

    fn sup_anchor(id: &str) -> InputAnchorSet {
        InputAnchorSet {
            observations: vec![],
            blobs: vec![],
            supplementals: vec![SupplementalId::new(id)],
        }
    }

    #[test]
    fn freshness_marks_threshold_misses_deterministically() {
        let projector = FreshnessProjector::new(
            vec![FreshnessThreshold {
                source_id: "sys:chatgpt".to_owned(),
                max_age_seconds: 36 * 3600,
            }],
            at(0) + chrono::Duration::hours(37),
        );
        let projection = projector.project_observations(&[observation("sys:chatgpt", at(0))]);
        assert_eq!(projection.sources[0].status, FreshnessStatus::Missing);
        assert_eq!(projection.missing.len(), 1);
    }

    #[test]
    fn freshness_replay_is_deterministic_for_different_input_orders() {
        let projector = FreshnessProjector::new(
            vec![FreshnessThreshold {
                source_id: "sys:chatgpt".to_owned(),
                max_age_seconds: 36 * 3600,
            }],
            at(0) + chrono::Duration::hours(1),
        );
        let records = vec![
            observation("sys:chatgpt", at(0)),
            observation("sys:chatgpt", at(30)),
        ];
        let mut reversed = records.clone();
        reversed.reverse();

        assert_eq!(
            serde_json::to_value(projector.project_observations(&records)).unwrap(),
            serde_json::to_value(projector.project_observations(&reversed)).unwrap()
        );
    }

    #[test]
    fn freshness_prefers_communication_channel_id_for_channel_sources() {
        let projector = FreshnessProjector::new(
            vec![FreshnessThreshold {
                source_id: "chan:gmail:inbox".to_owned(),
                max_age_seconds: 1800,
            }],
            at(0) + chrono::Duration::minutes(20),
        );
        let projection = projector.project_observations(&[communication_observation(
            "obs:gmail",
            "chan:gmail:inbox",
            at(0),
            at(0) + chrono::Duration::minutes(30),
        )]);

        assert_eq!(projection.sources[0].status, FreshnessStatus::Fresh);
    }

    #[test]
    fn freshness_uses_source_system_for_non_channel_imports() {
        let projector = FreshnessProjector::new(
            vec![FreshnessThreshold {
                source_id: "sys:claude-ai".to_owned(),
                max_age_seconds: 1800,
            }],
            at(0) + chrono::Duration::minutes(20),
        );
        let projection = projector.project_observations(&[observation_with_source_instance(
            "sys:claude-ai",
            "claude-personal",
            at(0),
        )]);

        assert_eq!(projection.sources[0].status, FreshnessStatus::Fresh);
    }

    #[test]
    fn resume_snapshot_folds_multiple_session_records_into_one_project() {
        let records = vec![
            supplemental(
                "sup:summary",
                "session-summary@1",
                serde_json::json!({"summary": "done", "project": "alpha"}),
                obs_anchor("obs:1"),
                at(1),
            ),
            supplemental(
                "sup:parking",
                "parking@1",
                serde_json::json!({"statement": "park", "resume_context": "ctx", "project": "alpha"}),
                obs_anchor("obs:1"),
                at(2),
            ),
        ];
        let projection = CognitionStateProjector::new(at(10)).resume_snapshot(&records);
        assert_eq!(projection.projects.len(), 1);
        assert_eq!(projection.projects[0].project, "alpha");
        assert_eq!(projection.projects[0].parkings.len(), 1);
    }

    #[test]
    fn resume_snapshot_replay_is_deterministic_and_keeps_latest_summary() {
        let records = vec![
            supplemental(
                "sup:summary-new",
                "session-summary@1",
                serde_json::json!({"summary": "new", "project": "alpha"}),
                obs_anchor("obs:1"),
                at(3),
            ),
            supplemental(
                "sup:summary-old",
                "session-summary@1",
                serde_json::json!({"summary": "old", "project": "alpha"}),
                obs_anchor("obs:1"),
                at(1),
            ),
            supplemental(
                "sup:parking",
                "parking@1",
                serde_json::json!({"statement": "park", "resume_context": "ctx", "project": "alpha"}),
                obs_anchor("obs:1"),
                at(2),
            ),
        ];
        let mut reversed = records.clone();
        reversed.reverse();

        let projection = CognitionStateProjector::new(at(10)).resume_snapshot(&records);
        let replayed = CognitionStateProjector::new(at(10)).resume_snapshot(&reversed);

        assert_eq!(
            serde_json::to_value(&projection).unwrap(),
            serde_json::to_value(&replayed).unwrap()
        );
        assert_eq!(
            projection.projects[0].session_summary.as_deref(),
            Some("new")
        );
    }

    #[test]
    fn card_queue_first_approval_wins_and_send_marks_sent() {
        let records = vec![
            supplemental(
                "sup:draft",
                "reply-draft@1",
                serde_json::json!({"channel": "slack", "recipient": "U1", "body": "hi", "drafted_at": at(1)}),
                obs_anchor("obs:message"),
                at(1),
            ),
            supplemental(
                "sup:approval-b",
                "reply-approval@1",
                serde_json::json!({"interface": "discord", "decision": "skipped", "decided_at": at(3), "actor": "user"}),
                sup_anchor("sup:draft"),
                at(3),
            ),
            supplemental(
                "sup:send",
                "send-record@1",
                serde_json::json!({"channel": "slack", "sent_at": at(5), "mode": "approved"}),
                sup_anchor("sup:draft"),
                at(5),
            ),
            supplemental(
                "sup:approval-c",
                "reply-approval@1",
                serde_json::json!({"interface": "slack", "decision": "approved", "decided_at": at(2), "actor": "user"}),
                sup_anchor("sup:draft"),
                at(2),
            ),
            supplemental(
                "sup:approval-a",
                "reply-approval@1",
                serde_json::json!({"interface": "tailscale", "decision": "skipped", "decided_at": at(4), "actor": "user"}),
                sup_anchor("sup:draft"),
                at(4),
            ),
        ];
        let projection = CardQueueProjector::new(at(10)).project_records(&records);
        assert_eq!(projection.cards[0].state, CardState::Sent);
        assert_eq!(
            projection.cards[0]
                .approval_id
                .as_ref()
                .map(SupplementalId::as_str),
            Some("sup:approval-c")
        );
        assert_eq!(
            projection.cards[0].approval_interface.as_deref(),
            Some("slack")
        );
        assert!(!projection.cards[0].automatic_send);
    }

    #[test]
    fn card_queue_derives_agent_name_from_created_by_and_serializes_it() {
        let mut draft = supplemental(
            "sup:draft-agent",
            "reply-draft@1",
            serde_json::json!({"channel": "slack", "recipient": "U1", "body": "hi", "drafted_at": at(1)}),
            obs_anchor("obs:message"),
            at(1),
        );
        draft.created_by = ActorRef::new("agent:Dawn");
        draft.lineage = Some(LineageRef::new(
            "nanihold/work-item/1/execution/2/agent/Nagi",
        ));

        let projection = CardQueueProjector::new(at(10)).project_records(&[draft]);

        assert_eq!(projection.cards[0].agent_name.as_deref(), Some("Dawn"));
        assert_eq!(
            serde_json::to_value(&projection).unwrap()["cards"][0]["agent_name"],
            "Dawn"
        );
    }

    #[test]
    fn card_queue_does_not_derive_agent_name_from_owner_created_by() {
        let mut draft = supplemental(
            "sup:draft-owner",
            "reply-draft@1",
            serde_json::json!({"channel": "slack", "recipient": "U1", "body": "hi", "drafted_at": at(1)}),
            obs_anchor("obs:message"),
            at(1),
        );
        draft.created_by = ActorRef::new("owner:yuji");

        let projection = CardQueueProjector::new(at(10)).project_records(&[draft]);

        assert_eq!(projection.cards[0].agent_name, None);
        assert_eq!(
            serde_json::to_value(&projection).unwrap()["cards"][0]["agent_name"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn card_queue_falls_back_to_agent_name_from_lineage() {
        let mut draft = supplemental(
            "sup:draft-lineage",
            "reply-draft@1",
            serde_json::json!({"channel": "slack", "recipient": "U1", "body": "hi", "drafted_at": at(1)}),
            obs_anchor("obs:message"),
            at(1),
        );
        draft.created_by = ActorRef::new("system:reply-drafter");
        draft.lineage = Some(LineageRef::new(
            "nanihold/work-item/3/execution/4/agent/Nagi",
        ));

        let projection = CardQueueProjector::new(at(10)).project_records(&[draft]);

        assert_eq!(projection.cards[0].agent_name.as_deref(), Some("Nagi"));
    }

    #[test]
    fn card_queue_skips_invalid_send_transition_and_replay_is_deterministic() {
        let records = vec![
            supplemental(
                "sup:draft",
                "reply-draft@1",
                serde_json::json!({"channel": "slack", "recipient": "U1", "body": "hi", "drafted_at": at(1)}),
                obs_anchor("obs:message"),
                at(1),
            ),
            supplemental(
                "sup:send",
                "send-record@1",
                serde_json::json!({"channel": "slack", "sent_at": at(2), "mode": "approved"}),
                sup_anchor("sup:draft"),
                at(2),
            ),
        ];
        let mut reversed = records.clone();
        reversed.reverse();

        let projection = CardQueueProjector::new(at(10)).project_records(&records);
        let replayed = CardQueueProjector::new(at(10)).project_records(&reversed);

        assert_eq!(
            serde_json::to_value(&projection).unwrap(),
            serde_json::to_value(&replayed).unwrap()
        );
        assert_eq!(projection.cards[0].state, CardState::Pending);
        assert!(
            projection
                .audit_log
                .iter()
                .any(|event| event.code == "invalid_send_transition")
        );
    }

    #[test]
    fn card_queue_incremental_reducer_matches_full_replay_after_every_record() {
        let draft_id = "sup:draft-incremental";
        let expiring_id = "sup:draft-expiring";
        let records = vec![
            supplemental(
                draft_id,
                "reply-draft@1",
                serde_json::json!({
                    "channel": "slack",
                    "recipient": "U01",
                    "body": "hello",
                    "expires_at": at(5),
                }),
                obs_anchor("obs:1"),
                at(1),
            ),
            supplemental(
                expiring_id,
                "reply-draft@1",
                serde_json::json!({
                    "channel": "slack",
                    "recipient": "U02",
                    "body": "expires",
                    "expires_at": at(4),
                }),
                obs_anchor("obs:2"),
                at(2),
            ),
            supplemental(
                "sup:approval-incremental",
                "reply-approval@1",
                serde_json::json!({"decision": "approved", "interface": "api"}),
                sup_anchor(draft_id),
                at(3),
            ),
            supplemental(
                "sup:send-incremental",
                "send-record@1",
                serde_json::json!({"mode": "approved", "sent_at": at(4)}),
                sup_anchor(draft_id),
                at(4),
            ),
        ];
        let now = at(10);
        let mut reducer = CardQueueReducer::default();
        let mut prefix = Vec::new();
        for record in records {
            prefix.push(record.clone());
            reducer.upsert_record(record);
            assert_eq!(
                serde_json::to_value(reducer.projection(now)).unwrap(),
                serde_json::to_value(CardQueueProjector::new(now).project_records(&prefix))
                    .unwrap()
            );
        }
        assert_eq!(
            reducer
                .projection(now)
                .cards
                .iter()
                .find(|card| card.draft_id.as_str() == expiring_id)
                .unwrap()
                .state,
            CardState::Expired
        );
    }

    #[test]
    fn reply_slo_incremental_join_index_matches_full_replay() {
        let incoming = communication_observation("obs:incoming", "chan:gmail", at(1), at(8));
        let records = vec![
            supplemental(
                "sup:draft-slo",
                "reply-draft@1",
                serde_json::json!({
                    "channel": "gmail",
                    "recipient": "sender@example.test",
                    "body": "reply",
                }),
                obs_anchor(incoming.id.as_str()),
                at(2),
            ),
            supplemental(
                "sup:send-slo-late",
                "send-record@1",
                serde_json::json!({"mode": "automatic", "sent_at": at(7)}),
                sup_anchor("sup:draft-slo"),
                at(7),
            ),
            supplemental(
                "sup:send-slo-earlier",
                "send-record@1",
                serde_json::json!({"mode": "automatic", "sent_at": at(6)}),
                sup_anchor("sup:draft-slo"),
                at(8),
            ),
        ];
        let now = at(10);
        let mut index = ReplySloJoinIndex::default();
        let mut prefix = Vec::new();
        for record in records {
            prefix.push(record.clone());
            index.upsert_record(record);
            assert_eq!(
                serde_json::to_value(
                    ReplySloProjector::new(now)
                        .project_observations(std::slice::from_ref(&incoming), &index)
                )
                .unwrap(),
                serde_json::to_value(
                    ReplySloProjector::new(now)
                        .project_records(std::slice::from_ref(&incoming), &prefix)
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn plan_state_excludes_superseded_decisions_and_calculates_ages() {
        let records = vec![
            supplemental(
                "sup:claim",
                "claim@1",
                serde_json::json!({"statement": "claim", "verification_mode": "check", "project": "alpha"}),
                obs_anchor("obs:message"),
                at(1),
            ),
            supplemental(
                "sup:parking",
                "parking@1",
                serde_json::json!({"statement": "park", "resume_context": "ctx", "project": "alpha"}),
                obs_anchor("obs:message"),
                at(2),
            ),
            supplemental(
                "sup:decision-old",
                "decision@1",
                serde_json::json!({"statement": "old decision", "rationale": "old", "project": "alpha"}),
                obs_anchor("obs:message"),
                at(3),
            ),
            supplemental(
                "sup:decision-new",
                "decision@1",
                serde_json::json!({
                    "statement": "new decision",
                    "rationale": "new",
                    "project": "alpha",
                    "supersedes": ["sup:decision-old"]
                }),
                obs_anchor("obs:message"),
                at(4),
            ),
        ];

        let projection = CognitionStateProjector::new(at(10)).plan_state(&records);

        assert_eq!(projection.projects.len(), 1);
        let project = &projection.projects[0];
        assert_eq!(project.project, "alpha");
        assert_eq!(project.open_claim_count, 1);
        assert_eq!(project.parking_count, 1);
        assert_eq!(project.oldest_open_claim_age_seconds, Some(9));
        assert_eq!(project.oldest_parking_age_seconds, Some(8));
        assert_eq!(project.decisions.len(), 1);
        assert_eq!(project.decisions[0].id.as_str(), "sup:decision-new");
    }

    #[test]
    fn reply_slo_matches_send_records_through_reply_draft_anchor() {
        let incoming = communication_observation(
            "obs:incoming",
            "chan:gmail:inbox",
            at(0),
            at(0) + chrono::Duration::minutes(30),
        );
        let records = vec![
            supplemental(
                "sup:draft",
                "reply-draft@1",
                serde_json::json!({
                    "channel": "gmail",
                    "recipient": "sender@example.test",
                    "body": "reply",
                    "drafted_at": at(5),
                }),
                obs_anchor("obs:incoming"),
                at(5),
            ),
            supplemental(
                "sup:send",
                "send-record@1",
                serde_json::json!({
                    "channel": "gmail",
                    "sent_at": at(20),
                    "mode": "approved",
                }),
                sup_anchor("sup:draft"),
                at(20),
            ),
        ];

        let projection = ReplySloProjector::new(at(40)).project_records(&[incoming], &records);

        assert_eq!(projection.rows[0].latency_seconds, Some(20));
        assert_eq!(projection.rows[0].status, ReplySloStatus::SentOnTime);
        assert!(projection.overdue.is_empty());
    }

    #[test]
    fn reply_slo_indexed_and_incremental_projection_match_full_rebuild() {
        let observations = vec![
            communication_observation(
                "obs:on-time",
                "chan:gmail:inbox",
                at(0),
                at(0) + chrono::Duration::minutes(30),
            ),
            communication_observation(
                "obs:late",
                "chan:gmail:inbox",
                at(1),
                at(1) + chrono::Duration::minutes(30),
            ),
            communication_observation(
                "obs:unsent",
                "chan:gmail:inbox",
                at(2),
                at(2) + chrono::Duration::minutes(30),
            ),
        ];
        let records = vec![
            supplemental(
                "sup:draft-on-time",
                "reply-draft@1",
                serde_json::json!({"drafted_at": at(3)}),
                obs_anchor("obs:on-time"),
                at(3),
            ),
            supplemental(
                "sup:draft-late",
                "reply-draft@1",
                serde_json::json!({"drafted_at": at(4)}),
                obs_anchor("obs:late"),
                at(4),
            ),
            supplemental(
                "sup:draft-unsent",
                "reply-draft@1",
                serde_json::json!({"drafted_at": at(5)}),
                obs_anchor("obs:unsent"),
                at(5),
            ),
            supplemental(
                "sup:send-on-time-later",
                "send-record@1",
                serde_json::json!({"sent_at": at(0) + chrono::Duration::minutes(25)}),
                sup_anchor("sup:draft-on-time"),
                at(20),
            ),
            supplemental(
                "sup:send-late",
                "send-record@1",
                serde_json::json!({"sent_at": at(1) + chrono::Duration::minutes(40)}),
                sup_anchor("sup:draft-late"),
                at(21),
            ),
            supplemental(
                "sup:send-on-time-earliest",
                "send-record@1",
                serde_json::json!({"sent_at": at(0) + chrono::Duration::minutes(20)}),
                sup_anchor("sup:draft-on-time"),
                at(22),
            ),
        ];
        let projector = ReplySloProjector::new(at(0) + chrono::Duration::hours(1));
        let full = projector.project_records(&observations, &records);

        let join_index = ReplySloJoinIndex::from_records(&records);
        let indexed = projector.project_observations(&observations, &join_index);
        assert_eq!(
            serde_json::to_value(&indexed).unwrap(),
            serde_json::to_value(&full).unwrap()
        );

        let mut incremental_index = ReplySloJoinIndex::default();
        for record in &records {
            incremental_index.upsert_record(record.clone());
        }
        let incremental = projector.project_observations(&observations, &incremental_index);
        assert_eq!(
            serde_json::to_value(&incremental).unwrap(),
            serde_json::to_value(&full).unwrap()
        );
        assert_eq!(incremental.rows[0].status, ReplySloStatus::SentOnTime);
        assert_eq!(incremental.rows[1].status, ReplySloStatus::SentLate);
        assert_eq!(incremental.rows[2].status, ReplySloStatus::Overdue);
        assert_eq!(incremental.overdue.len(), 2);

        let earlier_send = supplemental(
            "sup:send-temporary",
            "send-record@1",
            serde_json::json!({"sent_at": at(10)}),
            sup_anchor("sup:draft-on-time"),
            at(23),
        );
        incremental_index.upsert_record(earlier_send.clone());
        incremental_index.remove_record(&earlier_send.id);
        let after_remove = projector.project_observations(&observations, &incremental_index);
        assert_eq!(
            serde_json::to_value(&after_remove).unwrap(),
            serde_json::to_value(&indexed).unwrap()
        );
    }

    #[test]
    fn communication_projection_rebuild_is_deterministic_cp01() {
        let first = communication_observation("obs:first", "chan:mail", at(1), at(8));
        let second = communication_observation("obs:second", "chan:mail", at(2), at(9));
        let records = vec![supplemental(
            "sup:draft",
            "reply-draft@1",
            serde_json::json!({"drafted_at": at(3)}),
            obs_anchor("obs:first"),
            at(3),
        )];
        let join_index = ReplySloJoinIndex::from_records(&records);
        let forward = CommunicationProjectionState::from_observations(
            &[first.clone(), second.clone()],
            &join_index,
        );
        let reverse =
            CommunicationProjectionState::from_observations(&[second, first], &join_index);
        assert_eq!(
            serde_json::to_value(forward).unwrap(),
            serde_json::to_value(reverse).unwrap()
        );
    }

    #[test]
    fn communication_projection_fold_is_o1_and_evaluates_time_on_read_cp03() {
        let pending = communication_observation("obs:pending", "chan:mail", at(1), at(8));
        let overdue = communication_observation("obs:overdue", "chan:mail", at(2), at(3));
        let join_index = ReplySloJoinIndex::default();
        let mut state = CommunicationProjectionState::default();
        state.fold_observations(std::slice::from_ref(&pending), &join_index);
        assert_eq!(state.len(), 1);
        state.fold_observations(&[overdue], &join_index);
        assert_eq!(state.len(), 2);
        assert_eq!(
            state
                .project(at(7))
                .rows
                .iter()
                .find(|row| row.incoming_observation_id.as_str() == "obs:pending")
                .unwrap()
                .status,
            ReplySloStatus::Pending
        );
        assert_eq!(
            state
                .project(at(9))
                .rows
                .iter()
                .find(|row| row.incoming_observation_id.as_str() == "obs:overdue")
                .unwrap()
                .status,
            ReplySloStatus::Overdue
        );
    }

    #[test]
    fn communication_retraction_removes_target_in_incremental_fold() {
        let mut target = communication_observation("obs:target", "chan:mail", at(1), at(8));
        target.meta["object_id"] = serde_json::json!("gmail:message:target");
        let mut retraction = communication_observation("obs:retract", "chan:mail", at(2), at(9));
        retraction.meta["retracts"] = serde_json::json!({
            "source_object_id": "gmail:message:target"
        });
        let mut state = CommunicationProjectionState::default();
        let join_index = ReplySloJoinIndex::default();
        state.fold_observations(&[target, retraction], &join_index);
        assert!(state.is_empty());
    }

    #[test]
    fn communication_opt_out_removes_subject_records_incrementally() {
        let mut target = communication_observation("obs:target", "chan:mail", at(1), at(8));
        target.subject = EntityRef::new("person:1");
        let mut consent = observation("sys:lethe-governance", at(2));
        consent.id = ObservationId::new("obs:consent");
        consent.subject = EntityRef::new("person:1");
        consent.schema = SchemaRef::new("schema:consent-decision");
        consent.payload = serde_json::json!({
            "status": "opted_out",
            "identifier": "sender@example.test"
        });
        let mut state = CommunicationProjectionState::default();
        let join_index = ReplySloJoinIndex::default();
        state.fold_observations(&[target, consent], &join_index);
        assert!(state.is_empty());
    }

    #[test]
    fn communication_privacy_state_survives_restart_and_repulls_opt_out_targets() {
        let mut target = communication_observation("obs:target", "chan:mail", at(1), at(8));
        target.subject = EntityRef::new("person:1");
        let opt_out = consent_observation("person:1", "opted_out", at(2));
        let join_index = ReplySloJoinIndex::default();
        let mut state = CommunicationProjectionState::default();

        state.fold_observations(std::slice::from_ref(&target), &join_index);
        assert_eq!(state.len(), 1);

        let mut restored: CommunicationProjectionState =
            serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
        assert!(restored.observations.is_empty());

        // This is the reverse-index pull performed by the application after
        // restart, before folding the consent delta.
        restored.remember_observations(std::slice::from_ref(&target), &join_index);
        restored.fold_observations(std::slice::from_ref(&opt_out), &join_index);
        assert!(restored.is_empty());
        assert_eq!(
            serde_json::to_value(&restored).unwrap()["consent_by_subject"]["person:1"]["status"],
            "opted_out"
        );

        let mut new_target = communication_observation("obs:new-target", "chan:mail", at(3), at(9));
        new_target.subject = EntityRef::new("person:1");
        restored.fold_observations(std::slice::from_ref(&new_target), &join_index);
        assert!(restored.is_empty());
    }

    #[test]
    fn communication_reconsent_repulls_targets_after_restart() {
        let mut target = communication_observation("obs:target", "chan:mail", at(1), at(8));
        target.subject = EntityRef::new("person:1");
        let opt_out = consent_observation("person:1", "opted_out", at(2));
        let reconsent = consent_observation("person:1", "unrestricted", at(3));
        let join_index = ReplySloJoinIndex::default();
        let mut state = CommunicationProjectionState::default();
        state.fold_observations(&[target.clone(), opt_out], &join_index);
        assert!(state.is_empty());

        let mut restored: CommunicationProjectionState =
            serde_json::from_value(serde_json::to_value(&state).unwrap()).unwrap();
        restored.remember_observations(std::slice::from_ref(&target), &join_index);
        restored.fold_observations(std::slice::from_ref(&reconsent), &join_index);
        assert_eq!(restored.len(), 1);
    }

    #[test]
    fn communication_projection_accepts_v9_legacy_privacy_fields_for_rebuild() {
        let target = communication_observation("obs:target", "chan:mail", at(1), at(8));
        let state = CommunicationProjectionState::from_observations(
            std::slice::from_ref(&target),
            &ReplySloJoinIndex::default(),
        );
        let mut legacy = serde_json::to_value(state).unwrap();
        let object = legacy.as_object_mut().unwrap();
        object.remove("consent_by_subject");
        object.remove("consent_by_identifier");
        object.remove("observation_ids_by_privacy_key");
        object.insert(
            "opted_out_subjects".to_owned(),
            serde_json::json!(["person:legacy"]),
        );
        object.insert(
            "opted_out_sender_ids".to_owned(),
            serde_json::json!(["sender@example.test"]),
        );

        let restored: CommunicationProjectionState = serde_json::from_value(legacy).unwrap();
        assert_eq!(restored.len(), 1);
    }

    #[test]
    fn communication_reconsent_restores_incrementally_and_ignores_late_old_decision() {
        let mut target = communication_observation("obs:target", "chan:mail", at(1), at(8));
        target.subject = EntityRef::new("person:1");
        let opt_out = consent_observation("person:1", "opted_out", at(2));
        let reconsent = consent_observation("person:1", "unrestricted", at(3));
        let late_old_opt_out = consent_observation("person:1", "opted_out", at(2));
        let join_index = ReplySloJoinIndex::default();
        let mut state = CommunicationProjectionState::default();

        state.fold_observations(&[target, opt_out], &join_index);
        assert!(state.is_empty());
        state.fold_observations(&[reconsent], &join_index);
        assert_eq!(state.len(), 1);
        state.fold_observations(&[late_old_opt_out], &join_index);
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn communication_retraction_is_not_reversed_by_reconsent() {
        let mut target = communication_observation("obs:target", "chan:mail", at(1), at(8));
        target.subject = EntityRef::new("person:1");
        target.meta["object_id"] = serde_json::json!("mail:permanent");
        let mut retraction = communication_observation("obs:retract", "chan:mail", at(2), at(9));
        retraction.meta["retracts"] = serde_json::json!({
            "source_object_id": "mail:permanent"
        });
        let reconsent = consent_observation("person:1", "unrestricted", at(3));
        let join_index = ReplySloJoinIndex::default();
        let mut state = CommunicationProjectionState::default();

        state.fold_observations(&[target, retraction, reconsent], &join_index);
        assert!(state.is_empty());
    }

    #[test]
    fn communication_projection_matches_full_recalculation_at_fixed_times_cp02() {
        let observations = vec![
            communication_observation("obs:on-time", "chan:mail", at(0), at(30)),
            communication_observation("obs:late", "chan:mail", at(1), at(31)),
            communication_observation("obs:overdue", "chan:mail", at(2), at(3)),
            communication_observation("obs:pending", "chan:mail", at(3), at(50)),
            communication_observation("obs:tie", "chan:mail", at(4), at(30)),
        ];
        let records = vec![
            supplemental(
                "sup:draft-on-time",
                "reply-draft@1",
                serde_json::json!({}),
                obs_anchor("obs:on-time"),
                at(5),
            ),
            supplemental(
                "sup:draft-late",
                "reply-draft@1",
                serde_json::json!({}),
                obs_anchor("obs:late"),
                at(6),
            ),
            supplemental(
                "sup:draft-tie",
                "reply-draft@1",
                serde_json::json!({}),
                obs_anchor("obs:tie"),
                at(7),
            ),
            supplemental(
                "sup:send-on-time-late",
                "send-record@1",
                serde_json::json!({"sent_at": at(20)}),
                sup_anchor("sup:draft-on-time"),
                at(20),
            ),
            supplemental(
                "sup:send-on-time-early",
                "send-record@1",
                serde_json::json!({"sent_at": at(10)}),
                sup_anchor("sup:draft-on-time"),
                at(21),
            ),
            supplemental(
                "sup:send-late",
                "send-record@1",
                serde_json::json!({"sent_at": at(40)}),
                sup_anchor("sup:draft-late"),
                at(22),
            ),
        ];
        let join_index = ReplySloJoinIndex::from_records(&records);
        let state = CommunicationProjectionState::from_observations(&observations, &join_index);
        for now in [at(2), at(4), at(50), at(55)] {
            let expected = ReplySloProjector::new(now).project_records(&observations, &records);
            assert_eq!(
                serde_json::to_value(expected).unwrap(),
                serde_json::to_value(state.project(now)).unwrap()
            );
        }
        let projected = state.project(at(50));
        let row = |id: &str| {
            projected
                .rows
                .iter()
                .find(|row| row.incoming_observation_id.as_str() == id)
                .unwrap()
        };
        assert_eq!(row("obs:on-time").status, ReplySloStatus::SentOnTime);
        assert_eq!(row("obs:late").status, ReplySloStatus::SentLate);
        assert_eq!(row("obs:overdue").status, ReplySloStatus::Overdue);
        assert_eq!(row("obs:on-time").latency_seconds, Some(10));
        assert_eq!(
            projected.overdue[0].incoming_observation_id.as_str(),
            "obs:overdue"
        );
    }
}
