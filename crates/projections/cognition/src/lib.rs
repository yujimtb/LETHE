use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use lethe_core::domain::{
    Observation, ObservationId, ProjectionRef, SupplementalId, SupplementalRecord,
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
        let resume = self.resume_snapshot(records);
        let claim_queue = ClaimQueueProjector.project_records(records);
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
        let mut sorted = records.to_vec();
        sorted.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        let mut cards = BTreeMap::<String, ReplyCard>::new();
        let mut audit_log = Vec::new();
        for record in sorted
            .iter()
            .filter(|record| record.kind == "reply-draft@1")
        {
            let Some(channel) = string_field(&record.payload, "channel") else {
                continue;
            };
            let Some(recipient) = string_field(&record.payload, "recipient") else {
                continue;
            };
            let Some(body) = string_field(&record.payload, "body") else {
                continue;
            };
            cards.insert(
                record.id.as_str().to_owned(),
                ReplyCard {
                    draft_id: record.id.clone(),
                    channel: channel.to_owned(),
                    recipient: recipient.to_owned(),
                    body: body.to_owned(),
                    state: CardState::Pending,
                    created_at: record.created_at,
                    updated_at: record.created_at,
                    approval_id: None,
                    approval_interface: None,
                    sent_record_id: None,
                    automatic_send: false,
                },
            );
        }

        for record in sorted
            .iter()
            .filter(|record| record.kind != "reply-draft@1")
        {
            match record.kind.as_str() {
                "reply-approval@1" => apply_approval(record, &mut cards, &mut audit_log),
                "send-record@1" => apply_send(record, &mut cards, &mut audit_log),
                _ => {}
            }
        }

        for card in cards.values_mut() {
            if card.state == CardState::Pending
                && expires_at(records, &card.draft_id).is_some_and(|expires| expires < self.now)
            {
                card.state = CardState::Expired;
                card.updated_at = self.now;
            }
        }
        CardQueueProjection {
            cards: cards.into_values().collect(),
            audit_log,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplySloJoinIndex {
    draft_to_observation: HashMap<String, ObservationId>,
    sent_by_observation: HashMap<String, DateTime<Utc>>,
}

#[derive(Debug)]
pub struct ReplySloJoinIndexUpdate {
    affected_observation_id: Option<ObservationId>,
    rollback: ReplySloJoinIndexRollback,
}

#[derive(Debug)]
enum ReplySloJoinIndexRollback {
    Draft {
        draft_id: String,
        previous: Option<ObservationId>,
    },
    Sent {
        observation_id: String,
        previous: Option<DateTime<Utc>>,
    },
    None,
}

impl ReplySloJoinIndex {
    pub fn build(records: &[SupplementalRecord]) -> Self {
        let mut index = Self::default();
        for record in records
            .iter()
            .filter(|record| record.kind == "reply-draft@1")
        {
            index.index_draft(record);
        }
        for record in records
            .iter()
            .filter(|record| record.kind == "send-record@1")
        {
            index.index_send(record);
        }
        index
    }

    pub fn apply_append(&mut self, record: &SupplementalRecord) -> ReplySloJoinIndexUpdate {
        match record.kind.as_str() {
            "reply-draft@1" => {
                let Some(observation_id) = record.derived_from.observations.first().cloned() else {
                    return ReplySloJoinIndexUpdate::none();
                };
                let draft_id = record.id.as_str().to_owned();
                let previous = self
                    .draft_to_observation
                    .insert(draft_id.clone(), observation_id);
                ReplySloJoinIndexUpdate {
                    affected_observation_id: None,
                    rollback: ReplySloJoinIndexRollback::Draft { draft_id, previous },
                }
            }
            "send-record@1" => {
                let Some((observation_id, sent_at)) = self.send_join(record) else {
                    return ReplySloJoinIndexUpdate::none();
                };
                let observation_key = observation_id.as_str().to_owned();
                let previous = self.sent_by_observation.get(&observation_key).copied();
                self.sent_by_observation
                    .entry(observation_key.clone())
                    .and_modify(|current| *current = (*current).min(sent_at))
                    .or_insert(sent_at);
                ReplySloJoinIndexUpdate {
                    affected_observation_id: Some(observation_id),
                    rollback: ReplySloJoinIndexRollback::Sent {
                        observation_id: observation_key,
                        previous,
                    },
                }
            }
            _ => ReplySloJoinIndexUpdate::none(),
        }
    }

    pub fn rollback(&mut self, update: ReplySloJoinIndexUpdate) {
        match update.rollback {
            ReplySloJoinIndexRollback::Draft { draft_id, previous } => match previous {
                Some(previous) => {
                    self.draft_to_observation.insert(draft_id, previous);
                }
                None => {
                    self.draft_to_observation.remove(&draft_id);
                }
            },
            ReplySloJoinIndexRollback::Sent {
                observation_id,
                previous,
            } => match previous {
                Some(previous) => {
                    self.sent_by_observation.insert(observation_id, previous);
                }
                None => {
                    self.sent_by_observation.remove(&observation_id);
                }
            },
            ReplySloJoinIndexRollback::None => {}
        }
    }

    fn index_draft(&mut self, record: &SupplementalRecord) {
        if let Some(observation_id) = record.derived_from.observations.first() {
            self.draft_to_observation
                .insert(record.id.as_str().to_owned(), observation_id.clone());
        }
    }

    fn index_send(&mut self, record: &SupplementalRecord) {
        let Some((observation_id, sent_at)) = self.send_join(record) else {
            return;
        };
        self.sent_by_observation
            .entry(observation_id.as_str().to_owned())
            .and_modify(|current| *current = (*current).min(sent_at))
            .or_insert(sent_at);
    }

    fn send_join(&self, record: &SupplementalRecord) -> Option<(ObservationId, DateTime<Utc>)> {
        let draft_id = single_draft_anchor(record)?;
        let observation_id = self.draft_to_observation.get(draft_id.as_str())?.clone();
        let sent_at = string_field(&record.payload, "sent_at").and_then(parse_datetime)?;
        Some((observation_id, sent_at))
    }
}

impl ReplySloJoinIndexUpdate {
    pub fn affected_observation_id(&self) -> Option<&ObservationId> {
        self.affected_observation_id.as_ref()
    }

    fn none() -> Self {
        Self {
            affected_observation_id: None,
            rollback: ReplySloJoinIndexRollback::None,
        }
    }
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
        let join_index = ReplySloJoinIndex::build(records);
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

fn expires_at(records: &[SupplementalRecord], draft_id: &SupplementalId) -> Option<DateTime<Utc>> {
    records
        .iter()
        .find(|record| &record.id == draft_id)
        .and_then(|record| string_field(&record.payload, "expires_at"))
        .and_then(|raw| raw.parse::<DateTime<Utc>>().ok())
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
        ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability,
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

        let join_index = ReplySloJoinIndex::build(&records);
        let indexed = projector.project_observations(&observations, &join_index);
        assert_eq!(
            serde_json::to_value(&indexed).unwrap(),
            serde_json::to_value(&full).unwrap()
        );

        let mut incremental_index = ReplySloJoinIndex::default();
        for record in &records {
            incremental_index.apply_append(record);
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
        let update = incremental_index.apply_append(&earlier_send);
        incremental_index.rollback(update);
        assert_eq!(incremental_index, join_index);
    }
}
