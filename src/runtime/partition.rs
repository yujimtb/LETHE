//! M15 Runtime — partition log and keyspec definitions.
//!
//! The first rollout starts with a single physical leaf that is also the root.
//! The complete routing and identity keyspecs are still pinned from day one.

use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::Observation;

pub const PARTITION_EVENT_INITIALIZE: &str = "initialize";
pub const PARTITION_EVENT_SPLIT_PREPARE: &str = "split_prepare";
pub const PARTITION_EVENT_SPLIT_COMMIT: &str = "split_commit";
pub const PARTITION_EVENT_FAILOVER: &str = "failover";
pub const PARTITION_EVENT_RECOVER: &str = "recover";
pub const PARTITION_SPLIT_REASON_CAPACITY: &str = "capacity";

pub const ROUTING_KEYSPEC_VERSION: &str = "routing-keyspec/v1";
pub const IDENTITY_KEYSPEC_VERSION: &str = "identity-keyspec/v1";
const ROUTING_AXIS_SEPARATOR: char = '\u{1f}';

#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
    #[error("invalid leaf id: {0}")]
    InvalidLeafId(String),
    #[error("routing axis {axis} is empty")]
    EmptyRoutingAxis { axis: &'static str },
    #[error("routing axis {axis} contains the reserved separator")]
    ReservedRoutingAxisSeparator { axis: &'static str },
    #[error("routing key requires exactly 5 axes")]
    InvalidRoutingAxisCount,
    #[error("routing prefix cannot contain more than 5 axes")]
    InvalidRoutingPrefixAxisCount,
    #[error("partition log has no initialize event")]
    MissingInitialize,
    #[error("partition log has more than one initialize event")]
    DuplicateInitialize,
    #[error("split parent leaf not found: {0}")]
    SplitParentNotFound(String),
    #[error("split parent is already retired: {0}")]
    SplitParentAlreadyRetired(String),
    #[error("split child leaf id already exists: {0}")]
    DuplicateLeafId(String),
    #[error("split_commit reason must be capacity")]
    InvalidSplitReason,
    #[error("split capacity must be greater than zero")]
    InvalidSplitCapacity,
    #[error("capacity split requires at least two routed observations")]
    InsufficientSplitInput,
    #[error("routing keys have no discriminating bit")]
    NoDiscriminatingBit,
    #[error("split cutover step out of order: expected {expected}, actual {actual}")]
    SplitCutoverOutOfOrder {
        expected: &'static str,
        actual: &'static str,
    },
    #[error("unknown partition event type: {0}")]
    UnknownEventType(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutingKeySpec {
    pub version: &'static str,
    pub axes: Vec<RoutingAxisSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoutingAxisSpec {
    pub name: &'static str,
    pub source_field: &'static str,
    pub encoding: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentityKeySpec {
    pub version: &'static str,
    pub structure: &'static str,
    pub hash: &'static str,
    pub object_id_rule: &'static str,
    pub canonical_content: CanonicalContentSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalContentSpec {
    pub include: Vec<&'static str>,
    pub exclude: Vec<&'static str>,
    pub normalization: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializePartitionEvent {
    pub root_leaf_id: String,
    pub routing_keyspec_version: String,
    pub identity_keyspec_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitPreparePartitionEvent {
    pub parent_leaf_id: String,
    pub left_child_leaf_id: String,
    pub right_child_leaf_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitCommitPartitionEvent {
    pub parent_leaf_id: String,
    pub left_child_leaf_id: String,
    pub right_child_leaf_id: String,
    pub bit_index: u32,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverPartitionEvent {
    pub leaf_id: String,
    pub failover_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverPartitionEvent {
    pub leaf_id: String,
    pub failover_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionLogEvent {
    Initialize(InitializePartitionEvent),
    SplitPrepare(SplitPreparePartitionEvent),
    SplitCommit(SplitCommitPartitionEvent),
    Failover(FailoverPartitionEvent),
    Recover(RecoverPartitionEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingKey {
    axes: Vec<String>,
}

impl RoutingKey {
    pub fn from_axes(axes: Vec<String>) -> Result<Self, PartitionError> {
        if axes.len() != 5 {
            return Err(PartitionError::InvalidRoutingAxisCount);
        }

        for (axis_name, value) in [
            "coarse_month",
            "coarse_year",
            "source",
            "container",
            "fine_published",
        ]
        .into_iter()
        .zip(axes.iter())
        {
            validate_routing_axis(axis_name, value)?;
        }

        Ok(Self { axes })
    }

    pub fn axes(&self) -> &[String] {
        &self.axes
    }

    pub fn encoded(&self) -> String {
        self.axes.join(&ROUTING_AXIS_SEPARATOR.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTree {
    root: PartitionNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedObservation {
    pub observation_id: String,
    pub routing_key: RoutingKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RehomeTarget {
    pub observation_id: String,
    pub target_leaf_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacitySplitPlan {
    pub parent_leaf_id: String,
    pub left_child_leaf_id: String,
    pub right_child_leaf_id: String,
    pub bit_index: u32,
    pub rehome_targets: Vec<RehomeTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitCutoverStep {
    Prepared,
    CaughtUp,
    WriteBarrierEntered,
    Committed,
}

impl SplitCutoverStep {
    fn as_str(self) -> &'static str {
        match self {
            SplitCutoverStep::Prepared => "prepared",
            SplitCutoverStep::CaughtUp => "caught_up",
            SplitCutoverStep::WriteBarrierEntered => "write_barrier_entered",
            SplitCutoverStep::Committed => "committed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitCutoverProtocol {
    plan: CapacitySplitPlan,
    step: SplitCutoverStep,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PartitionNode {
    Leaf {
        leaf_id: String,
    },
    Split {
        parent_leaf_id: String,
        bit_index: u32,
        left: Box<PartitionNode>,
        right: Box<PartitionNode>,
    },
}

pub fn routing_keyspec() -> RoutingKeySpec {
    RoutingKeySpec {
        version: ROUTING_KEYSPEC_VERSION,
        axes: vec![
            RoutingAxisSpec {
                name: "coarse_month",
                source_field: "published",
                encoding: "utc_month_2digit",
            },
            RoutingAxisSpec {
                name: "coarse_year",
                source_field: "published",
                encoding: "utc_year_4digit",
            },
            RoutingAxisSpec {
                name: "source",
                source_field: "source_system",
                encoding: "stable_source_ref",
            },
            RoutingAxisSpec {
                name: "container",
                source_field: "source_container",
                encoding: "adapter_declared_workspace_or_channel",
            },
            RoutingAxisSpec {
                name: "fine_published",
                source_field: "published",
                encoding: "utc_rfc3339_nanos",
            },
        ],
    }
}

pub fn identity_keyspec() -> IdentityKeySpec {
    IdentityKeySpec {
        version: IDENTITY_KEYSPEC_VERSION,
        structure: "source:object_id:sha256(canonical_content)",
        hash: "sha256",
        object_id_rule: "adapter_declared_source_specific_object_id",
        canonical_content: CanonicalContentSpec {
            include: vec!["sender", "body", "event_time", "attachment_sha256"],
            exclude: vec!["reactions", "edit_wrapper", "ingestion_meta"],
            normalization: vec![
                "unicode_nfc",
                "crlf_to_lf",
                "json_canonical",
                "rfc3339_utc_fixed_precision",
            ],
        },
    }
}

pub fn routing_keyspec_json() -> Result<String, serde_json::Error> {
    serde_json::to_string(&routing_keyspec())
}

pub fn identity_keyspec_json() -> Result<String, serde_json::Error> {
    serde_json::to_string(&identity_keyspec())
}

pub fn initialize_event_json(root_leaf_id: &str) -> Result<String, serde_json::Error> {
    validate_leaf_id(root_leaf_id).expect("initialize root leaf id must be lake:<uuid>");
    serde_json::to_string(&InitializePartitionEvent {
        root_leaf_id: root_leaf_id.to_owned(),
        routing_keyspec_version: ROUTING_KEYSPEC_VERSION.to_owned(),
        identity_keyspec_version: IDENTITY_KEYSPEC_VERSION.to_owned(),
    })
}

pub fn split_prepare_event_json(
    parent_leaf_id: &str,
    left_child_leaf_id: &str,
    right_child_leaf_id: &str,
) -> Result<String, PartitionError> {
    validate_split_leaf_ids(parent_leaf_id, left_child_leaf_id, right_child_leaf_id)?;
    Ok(serde_json::to_string(&SplitPreparePartitionEvent {
        parent_leaf_id: parent_leaf_id.to_owned(),
        left_child_leaf_id: left_child_leaf_id.to_owned(),
        right_child_leaf_id: right_child_leaf_id.to_owned(),
        reason: PARTITION_SPLIT_REASON_CAPACITY.to_owned(),
    })?)
}

pub fn split_commit_event_json(
    parent_leaf_id: &str,
    left_child_leaf_id: &str,
    right_child_leaf_id: &str,
    bit_index: u32,
) -> Result<String, PartitionError> {
    validate_split_leaf_ids(parent_leaf_id, left_child_leaf_id, right_child_leaf_id)?;
    Ok(serde_json::to_string(&SplitCommitPartitionEvent {
        parent_leaf_id: parent_leaf_id.to_owned(),
        left_child_leaf_id: left_child_leaf_id.to_owned(),
        right_child_leaf_id: right_child_leaf_id.to_owned(),
        bit_index,
        reason: PARTITION_SPLIT_REASON_CAPACITY.to_owned(),
    })?)
}

pub fn failover_event_json(leaf_id: &str, failover_id: &str) -> Result<String, PartitionError> {
    validate_leaf_id(leaf_id)?;
    validate_failover_id(failover_id)?;
    Ok(serde_json::to_string(&FailoverPartitionEvent {
        leaf_id: leaf_id.to_owned(),
        failover_id: failover_id.to_owned(),
    })?)
}

pub fn recover_event_json(leaf_id: &str, failover_id: &str) -> Result<String, PartitionError> {
    validate_leaf_id(leaf_id)?;
    validate_failover_id(failover_id)?;
    Ok(serde_json::to_string(&RecoverPartitionEvent {
        leaf_id: leaf_id.to_owned(),
        failover_id: failover_id.to_owned(),
    })?)
}

pub fn parse_partition_event(
    event_type: &str,
    event_json: &str,
) -> Result<PartitionLogEvent, PartitionError> {
    match event_type {
        PARTITION_EVENT_INITIALIZE => {
            let event = serde_json::from_str::<InitializePartitionEvent>(event_json)?;
            validate_leaf_id(&event.root_leaf_id)?;
            Ok(PartitionLogEvent::Initialize(event))
        }
        PARTITION_EVENT_SPLIT_PREPARE => {
            let event = serde_json::from_str::<SplitPreparePartitionEvent>(event_json)?;
            validate_split_leaf_ids(
                &event.parent_leaf_id,
                &event.left_child_leaf_id,
                &event.right_child_leaf_id,
            )?;
            if event.reason != PARTITION_SPLIT_REASON_CAPACITY {
                return Err(PartitionError::InvalidSplitReason);
            }
            Ok(PartitionLogEvent::SplitPrepare(event))
        }
        PARTITION_EVENT_SPLIT_COMMIT => {
            let event = serde_json::from_str::<SplitCommitPartitionEvent>(event_json)?;
            validate_split_leaf_ids(
                &event.parent_leaf_id,
                &event.left_child_leaf_id,
                &event.right_child_leaf_id,
            )?;
            if event.reason != PARTITION_SPLIT_REASON_CAPACITY {
                return Err(PartitionError::InvalidSplitReason);
            }
            Ok(PartitionLogEvent::SplitCommit(event))
        }
        PARTITION_EVENT_FAILOVER => {
            let event = serde_json::from_str::<FailoverPartitionEvent>(event_json)?;
            validate_leaf_id(&event.leaf_id)?;
            validate_failover_id(&event.failover_id)?;
            Ok(PartitionLogEvent::Failover(event))
        }
        PARTITION_EVENT_RECOVER => {
            let event = serde_json::from_str::<RecoverPartitionEvent>(event_json)?;
            validate_leaf_id(&event.leaf_id)?;
            validate_failover_id(&event.failover_id)?;
            Ok(PartitionLogEvent::Recover(event))
        }
        other => Err(PartitionError::UnknownEventType(other.to_owned())),
    }
}

pub fn routing_key(
    published: DateTime<Utc>,
    source: &str,
    container: &str,
) -> Result<RoutingKey, PartitionError> {
    RoutingKey::from_axes(vec![
        format!("{:02}", published.month()),
        format!("{:04}", published.year()),
        source.to_owned(),
        container.to_owned(),
        published.to_rfc3339_opts(SecondsFormat::Nanos, true),
    ])
}

pub fn routing_key_from_observation(
    observation: &Observation,
) -> Result<RoutingKey, PartitionError> {
    let source = observation
        .source_system
        .as_ref()
        .ok_or(PartitionError::EmptyRoutingAxis { axis: "source" })?
        .as_str();
    let container = observation
        .meta
        .get("source_container")
        .and_then(serde_json::Value::as_str)
        .ok_or(PartitionError::EmptyRoutingAxis { axis: "container" })?;

    routing_key(observation.published, source, container)
}

pub fn plan_capacity_split(
    parent_leaf_id: &str,
    routed_observations: &[RoutedObservation],
    capacity: usize,
    left_child_leaf_id: &str,
    right_child_leaf_id: &str,
) -> Result<Option<CapacitySplitPlan>, PartitionError> {
    validate_split_leaf_ids(parent_leaf_id, left_child_leaf_id, right_child_leaf_id)?;
    if capacity == 0 {
        return Err(PartitionError::InvalidSplitCapacity);
    }
    if routed_observations.len() < capacity {
        return Ok(None);
    }
    if routed_observations.len() < 2 {
        return Err(PartitionError::InsufficientSplitInput);
    }

    let encoded_keys = routed_observations
        .iter()
        .map(|item| item.routing_key.encoded().into_bytes())
        .collect::<Vec<_>>();
    let bit_index = next_discriminating_bit(&encoded_keys)?;
    let mut rehome_targets = Vec::with_capacity(routed_observations.len());

    for item in routed_observations {
        let target_leaf_id = if bit_at(item.routing_key.encoded().as_bytes(), bit_index) {
            right_child_leaf_id
        } else {
            left_child_leaf_id
        };
        rehome_targets.push(RehomeTarget {
            observation_id: item.observation_id.clone(),
            target_leaf_id: target_leaf_id.to_owned(),
        });
    }

    Ok(Some(CapacitySplitPlan {
        parent_leaf_id: parent_leaf_id.to_owned(),
        left_child_leaf_id: left_child_leaf_id.to_owned(),
        right_child_leaf_id: right_child_leaf_id.to_owned(),
        bit_index,
        rehome_targets,
    }))
}

impl SplitCutoverProtocol {
    pub fn prepare(plan: CapacitySplitPlan) -> Self {
        Self {
            plan,
            step: SplitCutoverStep::Prepared,
        }
    }

    pub fn catch_up(&mut self) -> Result<(), PartitionError> {
        self.require_step(SplitCutoverStep::Prepared)?;
        self.step = SplitCutoverStep::CaughtUp;
        Ok(())
    }

    pub fn enter_write_barrier(&mut self) -> Result<(), PartitionError> {
        self.require_step(SplitCutoverStep::CaughtUp)?;
        self.step = SplitCutoverStep::WriteBarrierEntered;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<SplitCommitPartitionEvent, PartitionError> {
        self.require_step(SplitCutoverStep::WriteBarrierEntered)?;
        self.step = SplitCutoverStep::Committed;
        Ok(SplitCommitPartitionEvent {
            parent_leaf_id: self.plan.parent_leaf_id.clone(),
            left_child_leaf_id: self.plan.left_child_leaf_id.clone(),
            right_child_leaf_id: self.plan.right_child_leaf_id.clone(),
            bit_index: self.plan.bit_index,
            reason: PARTITION_SPLIT_REASON_CAPACITY.to_owned(),
        })
    }

    pub fn step(&self) -> SplitCutoverStep {
        self.step
    }

    pub fn plan(&self) -> &CapacitySplitPlan {
        &self.plan
    }

    fn require_step(&self, expected: SplitCutoverStep) -> Result<(), PartitionError> {
        if self.step == expected {
            Ok(())
        } else {
            Err(PartitionError::SplitCutoverOutOfOrder {
                expected: expected.as_str(),
                actual: self.step.as_str(),
            })
        }
    }
}

impl PartitionTree {
    pub fn from_events(events: &[PartitionLogEvent]) -> Result<Self, PartitionError> {
        let mut tree = None;

        for event in events {
            match event {
                PartitionLogEvent::Initialize(initialize) => {
                    if tree.is_some() {
                        return Err(PartitionError::DuplicateInitialize);
                    }
                    validate_leaf_id(&initialize.root_leaf_id)?;
                    tree = Some(Self {
                        root: PartitionNode::Leaf {
                            leaf_id: initialize.root_leaf_id.clone(),
                        },
                    });
                }
                PartitionLogEvent::SplitCommit(commit) => {
                    let current = tree.as_mut().ok_or(PartitionError::MissingInitialize)?;
                    current.apply_split_commit(commit)?;
                }
                PartitionLogEvent::SplitPrepare(_)
                | PartitionLogEvent::Failover(_)
                | PartitionLogEvent::Recover(_) => {}
            }
        }

        tree.ok_or(PartitionError::MissingInitialize)
    }

    pub fn root_leaf_id(&self) -> &str {
        match &self.root {
            PartitionNode::Leaf { leaf_id } => leaf_id,
            PartitionNode::Split { parent_leaf_id, .. } => parent_leaf_id,
        }
    }

    pub fn route<'a>(&'a self, key: &RoutingKey) -> &'a str {
        self.root.route(key.encoded().as_bytes())
    }

    pub fn current_leaf_ids(&self) -> Vec<String> {
        let mut leaves = Vec::new();
        self.root.collect_leaves(&mut leaves);
        leaves
    }

    pub fn candidate_leaf_ids_for_prefix_axes(
        &self,
        axes: &[String],
    ) -> Result<Vec<String>, PartitionError> {
        if axes.len() > 5 {
            return Err(PartitionError::InvalidRoutingPrefixAxisCount);
        }
        for (axis_name, value) in [
            "coarse_month",
            "coarse_year",
            "source",
            "container",
            "fine_published",
        ]
        .into_iter()
        .zip(axes.iter())
        {
            validate_routing_axis(axis_name, value)?;
        }
        if axes.is_empty() {
            return Ok(self.current_leaf_ids());
        }

        let prefix = axes.join(&ROUTING_AXIS_SEPARATOR.to_string());
        let encoded_prefix = if axes.len() < 5 {
            format!("{prefix}{ROUTING_AXIS_SEPARATOR}")
        } else {
            prefix
        };
        let mut leaves = Vec::new();
        self.root
            .collect_candidate_leaves(encoded_prefix.as_bytes(), &mut leaves);
        Ok(leaves)
    }

    pub fn apply_split_commit(
        &mut self,
        commit: &SplitCommitPartitionEvent,
    ) -> Result<(), PartitionError> {
        if commit.reason != PARTITION_SPLIT_REASON_CAPACITY {
            return Err(PartitionError::InvalidSplitReason);
        }
        validate_split_leaf_ids(
            &commit.parent_leaf_id,
            &commit.left_child_leaf_id,
            &commit.right_child_leaf_id,
        )?;
        if self.root.contains_current_leaf(&commit.left_child_leaf_id) {
            return Err(PartitionError::DuplicateLeafId(
                commit.left_child_leaf_id.clone(),
            ));
        }
        if self.root.contains_current_leaf(&commit.right_child_leaf_id) {
            return Err(PartitionError::DuplicateLeafId(
                commit.right_child_leaf_id.clone(),
            ));
        }

        if self.root.contains_retired_parent(&commit.parent_leaf_id) {
            return Err(PartitionError::SplitParentAlreadyRetired(
                commit.parent_leaf_id.clone(),
            ));
        }
        if self.root.replace_leaf_with_split(commit) {
            Ok(())
        } else {
            Err(PartitionError::SplitParentNotFound(
                commit.parent_leaf_id.clone(),
            ))
        }
    }
}

impl PartitionNode {
    fn route<'a>(&'a self, key_bytes: &[u8]) -> &'a str {
        match self {
            PartitionNode::Leaf { leaf_id } => leaf_id,
            PartitionNode::Split {
                bit_index,
                left,
                right,
                ..
            } => {
                if bit_at(key_bytes, *bit_index) {
                    right.route(key_bytes)
                } else {
                    left.route(key_bytes)
                }
            }
        }
    }

    fn collect_leaves(&self, leaves: &mut Vec<String>) {
        match self {
            PartitionNode::Leaf { leaf_id } => leaves.push(leaf_id.clone()),
            PartitionNode::Split { left, right, .. } => {
                left.collect_leaves(leaves);
                right.collect_leaves(leaves);
            }
        }
    }

    fn collect_candidate_leaves(&self, prefix: &[u8], leaves: &mut Vec<String>) {
        match self {
            PartitionNode::Leaf { leaf_id } => leaves.push(leaf_id.clone()),
            PartitionNode::Split {
                bit_index,
                left,
                right,
                ..
            } => {
                if prefix_covers_bit(prefix, *bit_index) {
                    if bit_at(prefix, *bit_index) {
                        right.collect_candidate_leaves(prefix, leaves);
                    } else {
                        left.collect_candidate_leaves(prefix, leaves);
                    }
                } else {
                    left.collect_leaves(leaves);
                    right.collect_leaves(leaves);
                }
            }
        }
    }

    fn contains_current_leaf(&self, needle: &str) -> bool {
        match self {
            PartitionNode::Leaf { leaf_id } => leaf_id == needle,
            PartitionNode::Split { left, right, .. } => {
                left.contains_current_leaf(needle) || right.contains_current_leaf(needle)
            }
        }
    }

    fn contains_retired_parent(&self, needle: &str) -> bool {
        match self {
            PartitionNode::Leaf { .. } => false,
            PartitionNode::Split {
                parent_leaf_id,
                left,
                right,
                ..
            } => {
                parent_leaf_id == needle
                    || left.contains_retired_parent(needle)
                    || right.contains_retired_parent(needle)
            }
        }
    }

    fn replace_leaf_with_split(&mut self, commit: &SplitCommitPartitionEvent) -> bool {
        match self {
            PartitionNode::Leaf { leaf_id } if leaf_id == &commit.parent_leaf_id => {
                *self = PartitionNode::Split {
                    parent_leaf_id: commit.parent_leaf_id.clone(),
                    bit_index: commit.bit_index,
                    left: Box::new(PartitionNode::Leaf {
                        leaf_id: commit.left_child_leaf_id.clone(),
                    }),
                    right: Box::new(PartitionNode::Leaf {
                        leaf_id: commit.right_child_leaf_id.clone(),
                    }),
                };
                true
            }
            PartitionNode::Leaf { .. } => false,
            PartitionNode::Split { left, right, .. } => {
                left.replace_leaf_with_split(commit) || right.replace_leaf_with_split(commit)
            }
        }
    }
}

fn bit_at(bytes: &[u8], bit_index: u32) -> bool {
    let byte_index = (bit_index / 8) as usize;
    let bit_in_byte = 7 - (bit_index % 8);
    bytes
        .get(byte_index)
        .map(|byte| (byte & (1 << bit_in_byte)) != 0)
        .unwrap_or(false)
}

fn prefix_covers_bit(prefix: &[u8], bit_index: u32) -> bool {
    let byte_index = (bit_index / 8) as usize;
    byte_index < prefix.len()
}

fn next_discriminating_bit(keys: &[Vec<u8>]) -> Result<u32, PartitionError> {
    if keys.len() < 2 {
        return Err(PartitionError::InsufficientSplitInput);
    }

    let max_bits = keys.iter().map(|key| key.len()).max().unwrap_or(0) * 8;
    for bit_index in 0..max_bits {
        let bit_index = u32::try_from(bit_index).expect("routing key bit length fits in u32");
        let first = bit_at(&keys[0], bit_index);
        if keys.iter().any(|key| bit_at(key, bit_index) != first) {
            return Ok(bit_index);
        }
    }

    Err(PartitionError::NoDiscriminatingBit)
}

fn validate_leaf_id(value: &str) -> Result<(), PartitionError> {
    let Some(uuid) = value.strip_prefix("lake:") else {
        return Err(PartitionError::InvalidLeafId(value.to_owned()));
    };
    uuid::Uuid::parse_str(uuid).map_err(|_| PartitionError::InvalidLeafId(value.to_owned()))?;
    Ok(())
}

fn validate_split_leaf_ids(
    parent_leaf_id: &str,
    left_child_leaf_id: &str,
    right_child_leaf_id: &str,
) -> Result<(), PartitionError> {
    validate_leaf_id(parent_leaf_id)?;
    validate_leaf_id(left_child_leaf_id)?;
    validate_leaf_id(right_child_leaf_id)?;
    if parent_leaf_id == left_child_leaf_id {
        return Err(PartitionError::DuplicateLeafId(
            left_child_leaf_id.to_owned(),
        ));
    }
    if parent_leaf_id == right_child_leaf_id || left_child_leaf_id == right_child_leaf_id {
        return Err(PartitionError::DuplicateLeafId(
            right_child_leaf_id.to_owned(),
        ));
    }
    Ok(())
}

fn validate_routing_axis(axis: &'static str, value: &str) -> Result<(), PartitionError> {
    if value.is_empty() {
        return Err(PartitionError::EmptyRoutingAxis { axis });
    }
    if value.contains(ROUTING_AXIS_SEPARATOR) {
        return Err(PartitionError::ReservedRoutingAxisSeparator { axis });
    }
    Ok(())
}

fn validate_failover_id(value: &str) -> Result<(), PartitionError> {
    let Some(uuid) = value.strip_prefix("spool:") else {
        return Err(PartitionError::InvalidLeafId(value.to_owned()));
    };
    uuid::Uuid::parse_str(uuid).map_err(|_| PartitionError::InvalidLeafId(value.to_owned()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf_id() -> String {
        format!("lake:{}", uuid::Uuid::now_v7())
    }

    #[test]
    fn routing_keyspec_pins_axis_order() {
        let axes = routing_keyspec()
            .axes
            .into_iter()
            .map(|axis| axis.name)
            .collect::<Vec<_>>();

        assert_eq!(
            axes,
            vec![
                "coarse_month",
                "coarse_year",
                "source",
                "container",
                "fine_published"
            ]
        );
    }

    #[test]
    fn identity_keyspec_pins_canonical_boundary() {
        let spec = identity_keyspec();

        assert_eq!(spec.structure, "source:object_id:sha256(canonical_content)");
        assert!(spec.canonical_content.include.contains(&"body"));
        assert!(spec.canonical_content.exclude.contains(&"reactions"));
        assert!(
            spec.canonical_content
                .normalization
                .contains(&"unicode_nfc")
        );
    }

    #[test]
    fn routing_key_uses_month_year_source_container_published_order() {
        let published = chrono::DateTime::parse_from_rfc3339("2026-04-03T01:02:03.123456789Z")
            .unwrap()
            .to_utc();
        let key = routing_key(published, "sys:slack", "channel:C01").unwrap();

        assert_eq!(
            key.axes(),
            &[
                "04".to_owned(),
                "2026".to_owned(),
                "sys:slack".to_owned(),
                "channel:C01".to_owned(),
                "2026-04-03T01:02:03.123456789Z".to_owned()
            ]
        );
    }

    #[test]
    fn split_prepare_does_not_change_replayed_tree() {
        let root = leaf_id();
        let left = leaf_id();
        let right = leaf_id();
        let initialize = parse_partition_event(
            PARTITION_EVENT_INITIALIZE,
            &initialize_event_json(&root).unwrap(),
        )
        .unwrap();
        let prepare = parse_partition_event(
            PARTITION_EVENT_SPLIT_PREPARE,
            &split_prepare_event_json(&root, &left, &right).unwrap(),
        )
        .unwrap();

        let tree = PartitionTree::from_events(&[initialize, prepare]).unwrap();

        assert_eq!(tree.current_leaf_ids(), vec![root]);
    }

    #[test]
    fn split_commit_replays_patricia_tree_and_routes_by_bit_index() {
        let root = leaf_id();
        let left = leaf_id();
        let right = leaf_id();
        let initialize = parse_partition_event(
            PARTITION_EVENT_INITIALIZE,
            &initialize_event_json(&root).unwrap(),
        )
        .unwrap();
        let commit = parse_partition_event(
            PARTITION_EVENT_SPLIT_COMMIT,
            &split_commit_event_json(&root, &left, &right, 0).unwrap(),
        )
        .unwrap();
        let key = RoutingKey::from_axes(vec![
            "04".to_owned(),
            "2026".to_owned(),
            "sys:slack".to_owned(),
            "channel:C01".to_owned(),
            "2026-04-03T01:02:03.000000000Z".to_owned(),
        ])
        .unwrap();

        let tree = PartitionTree::from_events(&[initialize, commit]).unwrap();

        assert_eq!(tree.current_leaf_ids(), vec![left.clone(), right]);
        assert_eq!(tree.route(&key), left);
    }

    #[test]
    fn split_commit_cannot_reuse_retired_parent() {
        let root = leaf_id();
        let left = leaf_id();
        let right = leaf_id();
        let next_left = leaf_id();
        let next_right = leaf_id();
        let initialize = parse_partition_event(
            PARTITION_EVENT_INITIALIZE,
            &initialize_event_json(&root).unwrap(),
        )
        .unwrap();
        let first = parse_partition_event(
            PARTITION_EVENT_SPLIT_COMMIT,
            &split_commit_event_json(&root, &left, &right, 2).unwrap(),
        )
        .unwrap();
        let second = parse_partition_event(
            PARTITION_EVENT_SPLIT_COMMIT,
            &split_commit_event_json(&root, &next_left, &next_right, 3).unwrap(),
        )
        .unwrap();

        let err = PartitionTree::from_events(&[initialize, first, second]).unwrap_err();

        assert!(matches!(err, PartitionError::SplitParentAlreadyRetired(_)));
    }

    #[test]
    fn capacity_split_is_lazy_and_rehomes_all_parent_contents() {
        let parent = leaf_id();
        let left = leaf_id();
        let right = leaf_id();
        let april = routing_key(
            chrono::DateTime::parse_from_rfc3339("2026-04-03T01:02:03Z")
                .unwrap()
                .to_utc(),
            "sys:slack",
            "channel:C01",
        )
        .unwrap();
        let may = routing_key(
            chrono::DateTime::parse_from_rfc3339("2026-05-03T01:02:03Z")
                .unwrap()
                .to_utc(),
            "sys:slack",
            "channel:C01",
        )
        .unwrap();
        let routed = vec![
            RoutedObservation {
                observation_id: "obs:1".to_owned(),
                routing_key: april,
            },
            RoutedObservation {
                observation_id: "obs:2".to_owned(),
                routing_key: may,
            },
        ];

        assert!(
            plan_capacity_split(&parent, &routed, 3, &left, &right)
                .unwrap()
                .is_none()
        );
        let plan = plan_capacity_split(&parent, &routed, 2, &left, &right)
            .unwrap()
            .unwrap();

        assert_eq!(plan.rehome_targets.len(), 2);
        assert!(
            plan.rehome_targets
                .iter()
                .any(|target| target.target_leaf_id == left)
        );
        assert!(
            plan.rehome_targets
                .iter()
                .any(|target| target.target_leaf_id == right)
        );
    }

    #[test]
    fn split_cutover_requires_prepare_catchup_barrier_commit_order() {
        let parent = leaf_id();
        let left = leaf_id();
        let right = leaf_id();
        let plan = CapacitySplitPlan {
            parent_leaf_id: parent.clone(),
            left_child_leaf_id: left.clone(),
            right_child_leaf_id: right.clone(),
            bit_index: 8,
            rehome_targets: vec![],
        };
        let mut protocol = SplitCutoverProtocol::prepare(plan);

        let err = protocol.commit().unwrap_err();
        assert!(matches!(err, PartitionError::SplitCutoverOutOfOrder { .. }));

        protocol.catch_up().unwrap();
        protocol.enter_write_barrier().unwrap();
        let commit = protocol.commit().unwrap();

        assert_eq!(protocol.step(), SplitCutoverStep::Committed);
        assert_eq!(commit.parent_leaf_id, parent);
        assert_eq!(commit.left_child_leaf_id, left);
        assert_eq!(commit.right_child_leaf_id, right);
        assert_eq!(commit.bit_index, 8);
        assert_eq!(commit.reason, PARTITION_SPLIT_REASON_CAPACITY);
    }
}
