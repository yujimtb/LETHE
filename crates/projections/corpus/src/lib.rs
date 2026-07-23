//! Access-controlled corpus projection for workspace search.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use chrono::{DateTime, Utc};
use lethe_core::domain::{
    ConsentDecision, Observation, ProjectionRef, RetractionTarget,
    consent_decision_from_observation, consent_decision_order, observation_privacy_keys,
};
use lethe_engine::projection::runner::Projector;
use regex::Regex;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

pub const CORPUS_PROJECTION_ID: &str = "proj:corpus";
pub const CORPUS_PROJECTOR_VERSION: u32 = 1;
const SUPPORTED_SOURCE_TYPES: &[&str] = &[
    "chatgpt",
    "claude-ai",
    "claude-code",
    "codex",
    "discord",
    "docs",
    "drive",
    "forms",
    "github-comment",
    "github-commit",
    "github-event",
    "github-issue",
    "github-pr",
    "gmail",
    "sheets",
    "slack",
    "slides",
];

pub fn supported_source_types() -> &'static [&'static str] {
    SUPPORTED_SOURCE_TYPES
}

#[derive(Debug, Clone)]
pub struct CorpusConfig {
    pub mode: CorpusMode,
    pub channel_allow_regex: Regex,
    pub channel_opt_in: HashSet<String>,
    pub exclude_bot_authors: bool,
    pub opt_out_people: HashSet<String>,
    pub allowed_folder_ids: HashSet<String>,
    pub broad_visibility_threshold: SharingThreshold,
    pub excluded_file_ids: HashSet<String>,
    pub exclude_form_response_sheets: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorpusMode {
    WorkspaceFiltered,
    PersonalAllText,
}

impl Default for CorpusConfig {
    fn default() -> Self {
        Self {
            mode: CorpusMode::WorkspaceFiltered,
            channel_allow_regex: Regex::new(r"^\d{3}_").expect("valid default channel regex"),
            channel_opt_in: HashSet::new(),
            exclude_bot_authors: true,
            opt_out_people: HashSet::new(),
            allowed_folder_ids: HashSet::new(),
            broad_visibility_threshold: SharingThreshold::Domain,
            excluded_file_ids: HashSet::new(),
            exclude_form_response_sheets: true,
        }
    }
}

impl CorpusConfig {
    /// Stable fingerprint for every policy input that can change Corpus exposure.
    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};

        fn sorted(values: &HashSet<String>) -> Vec<&str> {
            let mut values = values.iter().map(String::as_str).collect::<Vec<_>>();
            values.sort_unstable();
            values
        }

        let policy = serde_json::json!({
            "projector_version": CORPUS_PROJECTOR_VERSION,
            "mode": self.mode,
            "channel_allow_regex": self.channel_allow_regex.as_str(),
            "channel_opt_in": sorted(&self.channel_opt_in),
            "exclude_bot_authors": self.exclude_bot_authors,
            "opt_out_people": sorted(&self.opt_out_people),
            "allowed_folder_ids": sorted(&self.allowed_folder_ids),
            "broad_visibility_threshold": self.broad_visibility_threshold,
            "excluded_file_ids": sorted(&self.excluded_file_ids),
            "exclude_form_response_sheets": self.exclude_form_response_sheets,
        });
        let encoded = serde_json::to_vec(&policy).expect("Corpus policy serializes");
        format!("{:x}", Sha256::digest(encoded))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharingThreshold {
    SpecificUsers,
    Domain,
    AnyoneWithLink,
    Public,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorpusRecord {
    pub record_id: String,
    pub source_type: String,
    pub anchor_url: String,
    pub source_title: String,
    pub source_location: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub text: String,
    pub normalized_text: String,
    #[serde(default)]
    pub thread_ts: Option<String>,
    #[serde(default)]
    pub container: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrivacyValidationReport {
    pub observations_checked: usize,
    pub records_checked: usize,
}

#[derive(Debug, Clone)]
pub struct CorpusProjector {
    config: CorpusConfig,
}

impl CorpusProjector {
    pub fn new(config: CorpusConfig) -> Self {
        Self { config }
    }

    pub fn default_config() -> Self {
        Self::new(CorpusConfig::default())
    }

    pub fn personal_all_text_config() -> Self {
        Self::new(CorpusConfig {
            mode: CorpusMode::PersonalAllText,
            ..CorpusConfig::default()
        })
    }

    pub fn project_observations(&self, observations: &[Observation]) -> Vec<CorpusRecord> {
        let privacy = PrivacyFilter::from_observations(observations);
        let visible = observations
            .iter()
            .filter(|observation| privacy.visible(observation))
            .cloned()
            .collect::<Vec<_>>();
        match self.config.mode {
            CorpusMode::WorkspaceFiltered => self.project_workspace_filtered(&visible),
            CorpusMode::PersonalAllText => self.project_personal_all_text(&visible),
        }
    }

    /// Explicit operator-triggered full privacy validation.  The normal
    /// append path never invokes this corpus-wide scan.
    pub fn validate_on_demand_full(
        &self,
        observations: &[Observation],
    ) -> Result<PrivacyValidationReport, String> {
        let records = self.project_observations(observations);
        let privacy = PrivacyFilter::from_observations(observations);
        let mut record_ids = BTreeSet::new();
        for record in &records {
            if !record_ids.insert(record.record_id.clone()) {
                return Err(format!(
                    "privacy projection produced duplicate record {}",
                    record.record_id
                ));
            }
            let observation_id = record
                .metadata
                .get("observation_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("record {} lacks observation_id", record.record_id))?;
            let observation = observations
                .iter()
                .find(|observation| observation.id.as_str() == observation_id)
                .ok_or_else(|| {
                    format!(
                        "record {} references unknown observation {}",
                        record.record_id, observation_id
                    )
                })?;
            if !privacy.visible(observation) {
                return Err(format!(
                    "privacy projection exposed shielded observation {}",
                    observation.id
                ));
            }
        }
        Ok(PrivacyValidationReport {
            observations_checked: observations.len(),
            records_checked: records.len(),
        })
    }

    /// Projects one canonical Observation using the persisted workspace exclusion state.
    pub fn project_observation(
        &self,
        observation: &Observation,
        form_response_sheet_ids: &HashSet<String>,
    ) -> Vec<CorpusRecord> {
        if observation.schema.as_str() == "schema:consent-decision"
            || observation.meta.get("retracts").is_some()
        {
            return Vec::new();
        }
        let mut records = match self.config.mode {
            CorpusMode::WorkspaceFiltered => {
                if observation.schema.as_str() == "schema:bot-answer-log" {
                    Vec::new()
                } else {
                    match observation.schema.as_str() {
                        "schema:slack-message" => {
                            self.slack_record(observation).into_iter().collect()
                        }
                        "schema:workspace-object-snapshot" => {
                            self.workspace_records(observation, form_response_sheet_ids)
                        }
                        _ => Vec::new(),
                    }
                }
            }
            CorpusMode::PersonalAllText => personal_record(observation).into_iter().collect(),
        };
        records.sort_by(|left, right| {
            right
                .timestamp
                .cmp(&left.timestamp)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        records
    }

    fn project_workspace_filtered(&self, observations: &[Observation]) -> Vec<CorpusRecord> {
        let form_response_sheet_ids = observations
            .iter()
            .filter_map(linked_form_sheet_id)
            .collect::<HashSet<_>>();

        let mut records = observations
            .iter()
            .flat_map(|observation| self.project_observation(observation, &form_response_sheet_ids))
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .timestamp
                .cmp(&left.timestamp)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        records
    }

    fn project_personal_all_text(&self, observations: &[Observation]) -> Vec<CorpusRecord> {
        let mut records = observations
            .iter()
            .flat_map(|observation| self.project_observation(observation, &HashSet::new()))
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .timestamp
                .cmp(&left.timestamp)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        records
    }

    fn slack_record(&self, observation: &Observation) -> Option<CorpusRecord> {
        let channel = string_at(&observation.payload, &["channel_name"])?;
        let channel_id = string_at(&observation.payload, &["channel_id"]).unwrap_or(channel);
        let is_public = bool_at(&observation.payload, &["is_public_channel"]).unwrap_or(false);
        let opted_in = self.config.channel_opt_in.contains(channel)
            || self.config.channel_opt_in.contains(channel_id);
        if !is_public {
            return None;
        }
        if !opted_in && !self.config.channel_allow_regex.is_match(channel) {
            return None;
        }
        if self.config.exclude_bot_authors
            && (bool_at(&observation.payload, &["is_bot"]).unwrap_or(false)
                || string_at(&observation.payload, &["user_id"])
                    .is_some_and(|id| id.starts_with('B')))
        {
            return None;
        }
        if is_opted_out(
            &self.config.opt_out_people,
            &observation.payload,
            &["user_id", "email", "user_name"],
        ) {
            return None;
        }

        let text = string_at(&observation.payload, &["text"])
            .unwrap_or("")
            .to_owned();
        let ts = string_at(&observation.payload, &["ts"]).unwrap_or("");
        let thread_ts = string_at(&observation.payload, &["thread_ts"]).map(str::to_owned);
        let anchor = string_at(&observation.payload, &["permalink"])
            .map(str::to_owned)
            .unwrap_or_else(|| format!("slack://{channel_id}/{ts}"));
        Some(record(RecordInput {
            record_id: format!("corpus:slack:{channel_id}:{ts}"),
            source_type: "slack",
            anchor_url: anchor,
            source_title: channel.to_owned(),
            source_location: Some(format!("#{channel}")),
            timestamp: observation.published,
            text,
            thread_ts,
            container: Some(channel.to_owned()),
            metadata: serde_json::json!({
                "observation_id": observation.id,
                "source_object_id": observation
                    .meta
                    .get("object_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(observation.id.as_str()),
                "channel_id": channel_id,
                "author": string_at(&observation.payload, &["user_name"]),
            }),
        }))
    }

    fn workspace_records(
        &self,
        observation: &Observation,
        form_response_sheet_ids: &HashSet<String>,
    ) -> Vec<CorpusRecord> {
        let Some(service) = string_at(&observation.payload, &["artifact", "service"]) else {
            return vec![];
        };
        let object_type =
            string_at(&observation.payload, &["artifact", "objectType"]).unwrap_or("");
        if service == "forms" && object_type == "form-response-content" {
            return vec![];
        }
        if service == "sheets"
            && self.config.exclude_form_response_sheets
            && (bool_at(&observation.payload, &["metadata", "formResponseSheet"]).unwrap_or(false)
                || string_at(&observation.payload, &["artifact", "sourceObjectId"])
                    .is_some_and(|id| form_response_sheet_ids.contains(id)))
        {
            return vec![];
        }
        if service == "drive" && !self.drive_allowed(observation) {
            return vec![];
        }
        if is_opted_out(
            &self.config.opt_out_people,
            &observation.payload,
            &["relations.owner", "owner", "author"],
        ) {
            return vec![];
        }

        match service {
            "docs" => self.docs_records(observation),
            "sheets" => self.sheet_records(observation),
            "forms" => self.form_records(observation, object_type),
            "slides" => self.slide_records(observation),
            "drive" => self.drive_records(observation),
            _ => vec![],
        }
    }

    fn drive_allowed(&self, observation: &Observation) -> bool {
        let source_id = string_at(&observation.payload, &["artifact", "sourceObjectId"]);
        if source_id.is_some_and(|id| self.config.excluded_file_ids.contains(id)) {
            return false;
        }
        if self.config.allowed_folder_ids.is_empty() {
            return false;
        }
        let Some(parent_ids) = observation
            .payload
            .pointer("/metadata/parentIds")
            .and_then(serde_json::Value::as_array)
        else {
            return false;
        };
        let parent_allowed = parent_ids
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|parent| self.config.allowed_folder_ids.contains(parent));
        if !parent_allowed {
            return false;
        }
        let level = string_at(&observation.payload, &["metadata", "sharingLevel"])
            .and_then(parse_sharing_level)
            .unwrap_or(SharingThreshold::SpecificUsers);
        level >= self.config.broad_visibility_threshold
    }

    fn docs_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let base_url = canonical_uri(observation);
        let Some(chunks) = observation
            .payload
            .pointer("/native/chunks")
            .and_then(serde_json::Value::as_array)
        else {
            return vec![];
        };
        chunks
            .iter()
            .enumerate()
            .filter_map(|(idx, chunk)| {
                let text = string_at(chunk, &["text"]).unwrap_or("").to_owned();
                if text.trim().is_empty() {
                    return None;
                }
                let heading = string_at(chunk, &["heading"]).map(str::to_owned);
                let anchor = string_at(chunk, &["anchor"])
                    .map(|anchor| format!("{base_url}#{anchor}"))
                    .unwrap_or_else(|| base_url.clone());
                Some(record(RecordInput {
                    record_id: format!("corpus:docs:{}:{idx}", observation.id),
                    source_type: "docs",
                    anchor_url: anchor,
                    source_title: title.clone(),
                    source_location: heading,
                    timestamp: observation.published,
                    text,
                    thread_ts: None,
                    container: string_at(&observation.payload, &["artifact", "containerId"])
                        .map(str::to_owned),
                    metadata: serde_json::json!({
                        "observation_id": observation.id,
                        "source_object_id": source_object_id(observation),
                    }),
                }))
            })
            .collect()
    }

    fn sheet_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let base_url = canonical_uri(observation);
        let Some(tabs) = observation
            .payload
            .pointer("/native/tabs")
            .and_then(serde_json::Value::as_array)
        else {
            return vec![];
        };
        let mut records = Vec::new();
        for tab in tabs {
            let tab_name = string_at(tab, &["name"]).unwrap_or("Sheet");
            if let Some(rows) = tab.pointer("/rows").and_then(serde_json::Value::as_array) {
                for row in rows {
                    let row_number = row
                        .get("rowNumber")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let text = cells_to_text(row);
                    if text.trim().is_empty() {
                        continue;
                    }
                    records.push(record(RecordInput {
                        record_id: format!(
                            "corpus:sheets:{}:{tab_name}:{row_number}",
                            observation.id
                        ),
                        source_type: "sheets",
                        anchor_url: format!(
                            "{base_url}#gid={tab_name}&range={row_number}:{row_number}"
                        ),
                        source_title: title.clone(),
                        source_location: Some(format!("{tab_name} row {row_number}")),
                        timestamp: observation.published,
                        text,
                        thread_ts: None,
                        container: Some(tab_name.to_owned()),
                        metadata: serde_json::json!({
                            "observation_id": observation.id,
                            "source_object_id": source_object_id(observation),
                        }),
                    }));
                }
            }
        }
        records
    }

    fn form_records(&self, observation: &Observation, object_type: &str) -> Vec<CorpusRecord> {
        let title = title(observation);
        let base_url = canonical_uri(observation);
        let text = if object_type == "form-response-fact" {
            format!(
                "{} answered at {}",
                string_at(&observation.payload, &["response", "respondent"]).unwrap_or("unknown"),
                string_at(&observation.payload, &["response", "submittedAt"]).unwrap_or("")
            )
        } else {
            let questions = observation
                .payload
                .pointer("/native/questions")
                .and_then(serde_json::Value::as_array)
                .map(|questions| {
                    questions
                        .iter()
                        .filter_map(|q| string_at(q, &["title"]))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            format!("{}\n{}", title, questions)
        };
        vec![record(RecordInput {
            record_id: format!("corpus:forms:{}:{object_type}", observation.id),
            source_type: "forms",
            anchor_url: base_url,
            source_title: title,
            source_location: Some(object_type.to_owned()),
            timestamp: observation.published,
            text,
            thread_ts: None,
            container: None,
            metadata: serde_json::json!({
                "observation_id": observation.id,
                "source_object_id": source_object_id(observation),
                "linked_sheet_id": linked_form_sheet_id(observation),
            }),
        })]
    }

    fn slide_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let url = canonical_uri(observation);
        let text = observation
            .payload
            .pointer("/native/slides")
            .and_then(serde_json::Value::as_array)
            .map(|slides| slides.iter().map(value_text).collect::<Vec<_>>().join("\n"))
            .unwrap_or_else(|| value_text(&observation.payload));
        vec![record(RecordInput {
            record_id: format!("corpus:slides:{}", observation.id),
            source_type: "slides",
            anchor_url: url,
            source_title: title,
            source_location: None,
            timestamp: observation.published,
            text,
            thread_ts: None,
            container: string_at(&observation.payload, &["artifact", "containerId"])
                .map(str::to_owned),
            metadata: serde_json::json!({
                "observation_id": observation.id,
                "source_object_id": source_object_id(observation),
            }),
        })]
    }

    fn drive_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let text = string_at(&observation.payload, &["native", "text"])
            .map(str::to_owned)
            .unwrap_or_else(|| title.clone());
        vec![record(RecordInput {
            record_id: format!("corpus:drive:{}", observation.id),
            source_type: "drive",
            anchor_url: canonical_uri(observation),
            source_title: title,
            source_location: string_at(&observation.payload, &["artifact", "objectType"])
                .map(str::to_owned),
            timestamp: observation.published,
            text,
            thread_ts: None,
            container: string_at(&observation.payload, &["artifact", "containerId"])
                .map(str::to_owned),
            metadata: serde_json::json!({
                "observation_id": observation.id,
                "source_object_id": source_object_id(observation),
                "sharing_level": string_at(&observation.payload, &["metadata", "sharingLevel"]),
            }),
        })]
    }
}

/// Incremental corpus fold.  A retraction removes already-materialized
/// records in the same fold; it never mutates the canonical observation.
#[derive(Debug, Default)]
pub struct CorpusProjectionState {
    observations: BTreeMap<String, Observation>,
    observation_ids_by_privacy_key: BTreeMap<String, BTreeSet<String>>,
    records: BTreeMap<String, CorpusRecord>,
    record_observation_ids: BTreeMap<String, String>,
    record_source_object_ids: BTreeMap<String, String>,
    privacy: PrivacyFilter,
}

impl CorpusProjectionState {
    pub fn fold_observations(
        &mut self,
        projector: &CorpusProjector,
        observations: &[Observation],
    ) -> Vec<CorpusRecord> {
        for observation in observations {
            self.observations
                .insert(observation.id.as_str().to_owned(), observation.clone());
            for privacy_key in observation_privacy_keys(observation) {
                self.observation_ids_by_privacy_key
                    .entry(privacy_key)
                    .or_default()
                    .insert(observation.id.as_str().to_owned());
            }

            if let Some(value) = observation.meta.get("retracts") {
                match RetractionTarget::from_value(value) {
                    Ok(target) => {
                        self.privacy.apply_retraction(&target);
                        self.remove_records_for_retraction(&target);
                    }
                    Err(_) => self.privacy.invalid_retraction = true,
                }
                continue;
            }

            if observation.schema.as_str() == "schema:consent-decision" {
                for privacy_key in self.privacy.apply_consent(observation) {
                    self.rematerialize_privacy_key(projector, &privacy_key);
                }
                continue;
            }

            self.remove_records_for_observation(observation.id.as_str());
            if !self.privacy.visible(observation) {
                continue;
            }
            for record in projector.project_observation(observation, &HashSet::new()) {
                self.insert_record(record);
            }
        }
        self.records.values().cloned().collect()
    }

    fn insert_record(&mut self, record: CorpusRecord) {
        self.remove_record(&record.record_id);
        let record_id = record.record_id.clone();
        if let Some(observation_id) = record
            .metadata
            .get("observation_id")
            .and_then(serde_json::Value::as_str)
        {
            self.record_observation_ids
                .insert(record_id.clone(), observation_id.to_owned());
        }
        if let Some(source_object_id) = record
            .metadata
            .get("source_object_id")
            .and_then(serde_json::Value::as_str)
        {
            self.record_source_object_ids
                .insert(record_id.clone(), source_object_id.to_owned());
        }
        self.records.insert(record_id, record);
    }

    fn remove_record(&mut self, record_id: &str) {
        self.records.remove(record_id);
        self.record_observation_ids.remove(record_id);
        self.record_source_object_ids.remove(record_id);
    }

    fn remove_records_for_observation(&mut self, observation_id: &str) {
        let record_ids = self
            .record_observation_ids
            .iter()
            .filter(|(_, value)| value.as_str() == observation_id)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for record_id in record_ids {
            self.remove_record(&record_id);
        }
    }

    fn remove_records_for_retraction(&mut self, target: &RetractionTarget) {
        let record_ids = self
            .records
            .keys()
            .filter(|record_id| {
                target.observation_id.as_ref().is_some_and(|id| {
                    self.record_observation_ids
                        .get(*record_id)
                        .is_some_and(|value| value == id.as_str())
                }) || target
                    .source_object_id
                    .as_ref()
                    .is_some_and(|id| self.record_source_object_ids.get(*record_id) == Some(id))
            })
            .cloned()
            .collect::<Vec<_>>();
        for record_id in record_ids {
            self.remove_record(&record_id);
        }
    }

    fn rematerialize_privacy_key(&mut self, projector: &CorpusProjector, privacy_key: &str) {
        let observation_ids = self
            .observation_ids_by_privacy_key
            .get(privacy_key)
            .cloned()
            .unwrap_or_default();
        for observation_id in observation_ids {
            self.remove_records_for_observation(&observation_id);
            let Some(observation) = self.observations.get(&observation_id).cloned() else {
                continue;
            };
            if !self.privacy.visible(&observation) {
                continue;
            }
            for record in projector.project_observation(&observation, &HashSet::new()) {
                self.insert_record(record);
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PrivacyFilter {
    retracted_observation_ids: BTreeSet<String>,
    retracted_source_object_ids: BTreeSet<String>,
    consent_by_subject: BTreeMap<String, ConsentDecision>,
    consent_by_identifier: BTreeMap<String, ConsentDecision>,
    invalid_retraction: bool,
}

impl PrivacyFilter {
    pub fn from_materialized_state(
        consent_by_subject: &BTreeMap<String, ConsentDecision>,
        consent_by_identifier: &BTreeMap<String, ConsentDecision>,
        retracted_observation_ids: &BTreeSet<String>,
        retracted_source_object_ids: &BTreeSet<String>,
    ) -> Self {
        Self {
            retracted_observation_ids: retracted_observation_ids.clone(),
            retracted_source_object_ids: retracted_source_object_ids.clone(),
            consent_by_subject: consent_by_subject.clone(),
            consent_by_identifier: consent_by_identifier.clone(),
            invalid_retraction: false,
        }
    }

    pub fn from_observations(observations: &[Observation]) -> Self {
        let mut fold = Self::default();
        fold.apply_observations(observations);
        fold
    }

    pub fn apply_observations(&mut self, observations: &[Observation]) {
        for observation in observations {
            if let Some(value) = observation.meta.get("retracts") {
                match RetractionTarget::from_value(value) {
                    Ok(target) => self.apply_retraction(&target),
                    Err(_) => self.invalid_retraction = true,
                }
            }
            self.apply_consent(observation);
        }
    }

    fn apply_retraction(&mut self, target: &RetractionTarget) {
        if let Some(id) = &target.observation_id {
            self.retracted_observation_ids
                .insert(id.as_str().to_owned());
        }
        if let Some(id) = &target.source_object_id {
            self.retracted_source_object_ids.insert(id.clone());
        }
    }

    /// Apply a consent ledger entry and return only keys whose latest entry
    /// changed.  Older decisions arriving late are intentionally ignored.
    pub fn apply_consent(&mut self, observation: &Observation) -> BTreeSet<String> {
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

    pub fn visible(&self, observation: &Observation) -> bool {
        if self.invalid_retraction {
            return false;
        }
        if observation.schema.as_str() == "schema:consent-decision"
            || observation.meta.get("retracts").is_some()
        {
            return false;
        }
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
        self.latest_consent(observation)
            .is_none_or(|decision| decision.status != "opted_out")
    }

    pub fn latest_consent(&self, observation: &Observation) -> Option<&ConsentDecision> {
        let identifiers = observation_identifiers(observation);
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

fn observation_identifiers(observation: &Observation) -> Vec<String> {
    observation_privacy_keys(observation)
        .into_iter()
        .filter(|key| key != observation.subject.as_str())
        .collect()
}

impl Projector for CorpusProjector {
    type Input = Observation;
    type Output = CorpusRecord;

    fn project(&self, inputs: &[Observation]) -> Vec<CorpusRecord> {
        self.project_observations(inputs)
    }
}

pub fn normalized_text(text: &str) -> String {
    text.nfkc().collect()
}

pub fn projection_watermark(records: &[CorpusRecord]) -> String {
    let mut latest: Option<DateTime<Utc>> = None;
    for record in records {
        latest = Some(latest.map_or(record.timestamp, |current| current.max(record.timestamp)));
    }
    format!(
        "{}:{}",
        ProjectionRef::new(CORPUS_PROJECTION_ID),
        latest
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "empty".to_owned())
    )
}

struct RecordInput<'a> {
    record_id: String,
    source_type: &'a str,
    anchor_url: String,
    source_title: String,
    source_location: Option<String>,
    timestamp: DateTime<Utc>,
    text: String,
    thread_ts: Option<String>,
    container: Option<String>,
    metadata: serde_json::Value,
}

fn record(input: RecordInput<'_>) -> CorpusRecord {
    CorpusRecord {
        record_id: input.record_id,
        source_type: input.source_type.to_owned(),
        anchor_url: input.anchor_url,
        source_title: input.source_title,
        source_location: input.source_location,
        timestamp: input.timestamp,
        normalized_text: normalized_text(&input.text),
        text: input.text,
        thread_ts: input.thread_ts,
        container: input.container,
        metadata: input.metadata,
    }
}

fn personal_record(observation: &Observation) -> Option<CorpusRecord> {
    let source_type = personal_source_type(observation);
    let text = personal_text(observation, &source_type)?;
    if text.trim().is_empty() {
        return None;
    }
    let thread_key = personal_thread_key(observation, &source_type);
    let metadata = personal_metadata(observation, &source_type, thread_key.as_deref());

    Some(record(RecordInput {
        record_id: format!("corpus:{source_type}:{}", observation.id.as_str()),
        source_type: &source_type,
        anchor_url: personal_anchor_url(observation, &source_type),
        source_title: personal_title(observation, &source_type),
        source_location: personal_location(observation, &source_type),
        timestamp: observation.published,
        text,
        thread_ts: thread_key,
        container: personal_container(observation, &source_type),
        metadata,
    }))
}

fn personal_source_type(observation: &Observation) -> String {
    let source_system = observation
        .source_system
        .as_ref()
        .map(|source| source.as_str());
    let schema = observation.schema.as_str();

    if matches!(source_system, Some("sys:claude-ai")) || schema == "schema:claude-message" {
        return "claude-ai".to_owned();
    }
    if matches!(source_system, Some("sys:github")) || schema == "schema:github-event" {
        return github_source_type(&observation.payload).to_owned();
    }
    if matches!(source_system, Some("sys:claude-code")) {
        return "claude-code".to_owned();
    }
    if matches!(source_system, Some("sys:codex")) {
        return "codex".to_owned();
    }
    if matches!(source_system, Some("sys:gmail")) || schema == "schema:gmail-message" {
        return "gmail".to_owned();
    }
    if matches!(source_system, Some("sys:discord")) || schema == "schema:discord-message" {
        return "discord".to_owned();
    }

    source_system
        .and_then(|source| source.strip_prefix("sys:"))
        .map(str::to_owned)
        .unwrap_or_else(|| schema.strip_prefix("schema:").unwrap_or(schema).to_owned())
}

fn github_source_type(payload: &serde_json::Value) -> &'static str {
    match string_at(payload, &["object_type"]).unwrap_or("") {
        "issue" => "github-issue",
        "pull_request" => "github-pr",
        "issue_comment" | "pull_request_review" | "pull_request_review_comment" => "github-comment",
        "commit" => "github-commit",
        _ => "github-event",
    }
}

fn personal_text(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "claude-ai" => string_at(&observation.payload, &["text"]).map(str::to_owned),
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            github_text(&observation.payload)
        }
        "claude-code" | "codex" => coding_agent_text(&observation.payload),
        "gmail" => gmail_text(&observation.payload),
        "discord" => string_at(&observation.payload, &["content"]).map(str::to_owned),
        _ => {
            let text = value_text(&observation.payload);
            (!text.trim().is_empty()).then_some(text)
        }
    }
}

fn gmail_text(payload: &serde_json::Value) -> Option<String> {
    let parts = [
        string_at(payload, &["subject"]).unwrap_or(""),
        string_at(payload, &["text"]).unwrap_or(""),
    ]
    .into_iter()
    .filter(|part| !part.trim().is_empty())
    .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn github_text(payload: &serde_json::Value) -> Option<String> {
    let parts = match string_at(payload, &["object_type"]).unwrap_or("") {
        "issue" | "pull_request" => vec![
            string_at(payload, &["title"]).unwrap_or(""),
            string_at(payload, &["body"]).unwrap_or(""),
        ],
        "issue_comment" | "pull_request_review_comment" => {
            vec![string_at(payload, &["body"]).unwrap_or("")]
        }
        "pull_request_review" => vec![
            string_at(payload, &["state"]).unwrap_or(""),
            string_at(payload, &["body"]).unwrap_or(""),
        ],
        "commit" => vec![string_at(payload, &["message"]).unwrap_or("")],
        _ => return Some(value_text(payload)),
    };
    let text = parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn coding_agent_text(payload: &serde_json::Value) -> Option<String> {
    let item = payload.get("item")?;
    let mut parts = match string_at(item, &["kind"])? {
        "message" => string_at(item, &["text"])
            .filter(|value| !value.trim().is_empty())
            .map(|value| vec![value.to_owned()])?,
        "tool_call" => string_at(item, &["tool_name"])
            .filter(|value| !value.trim().is_empty())
            .map(|value| vec![value.to_owned()])
            .unwrap_or_default(),
        _ => return None,
    };
    if let Some(references) = item.get("references") {
        collect_reference_text(references, &mut parts);
    }
    let text = parts.join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn collect_reference_text(value: &serde_json::Value, parts: &mut Vec<String>) {
    match value {
        serde_json::Value::String(text) => {
            if !text.trim().is_empty() {
                parts.push(text.to_owned());
            }
        }
        serde_json::Value::Number(number) => parts.push(number.to_string()),
        serde_json::Value::Bool(flag) => parts.push(flag.to_string()),
        serde_json::Value::Array(values) => {
            values
                .iter()
                .for_each(|value| collect_reference_text(value, parts));
        }
        serde_json::Value::Object(map) => {
            map.values()
                .for_each(|value| collect_reference_text(value, parts));
        }
        serde_json::Value::Null => {}
    }
}

fn personal_anchor_url(observation: &Observation, source_type: &str) -> String {
    if let Some(url) = string_at(&observation.payload, &["html_url"])
        .or_else(|| string_at(&observation.payload, &["url"]))
        .or_else(|| string_at(&observation.payload, &["permalink"]))
        .or_else(|| string_at(&observation.payload, &["artifact", "canonicalUri"]))
    {
        return url.to_owned();
    }

    match source_type {
        "claude-ai" => {
            let conversation = string_at(&observation.payload, &["conversation_uuid"])
                .unwrap_or(observation.id.as_str());
            let message = string_at(&observation.payload, &["message_uuid"]).unwrap_or("");
            format!("claude-ai://conversation/{conversation}/message/{message}")
        }
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            let repo = string_at(&observation.payload, &["repo"]).unwrap_or("unknown");
            let object_type = string_at(&observation.payload, &["object_type"]).unwrap_or("event");
            let object_id = string_at(&observation.payload, &["sha"])
                .map(str::to_owned)
                .or_else(|| number_like(&observation.payload, "number"))
                .or_else(|| number_like(&observation.payload, "id"))
                .or_else(|| string_at(&observation.payload, &["event_key"]).map(str::to_owned))
                .unwrap_or_else(|| observation.id.as_str().to_owned());
            format!("github://{repo}/{object_type}/{object_id}")
        }
        "claude-code" | "codex" => {
            let session = coding_session_id(&observation.payload)
                .unwrap_or_else(|| observation.id.as_str().to_owned());
            let message = coding_message_id(&observation.payload)
                .unwrap_or_else(|| observation.id.as_str().to_owned());
            format!("{source_type}://session/{session}/message/{message}")
        }
        "gmail" => {
            let account = string_at(&observation.payload, &["account_id"]).unwrap_or("unknown");
            let message =
                string_at(&observation.payload, &["message_id"]).unwrap_or(observation.id.as_str());
            format!("gmail://{account}/message/{message}")
        }
        "discord" => {
            let channel = string_at(&observation.payload, &["channel_id"]).unwrap_or("unknown");
            let message =
                string_at(&observation.payload, &["message_id"]).unwrap_or(observation.id.as_str());
            format!("discord://{channel}/message/{message}")
        }
        _ => format!("observation://{}", observation.id.as_str()),
    }
}

fn personal_title(observation: &Observation, source_type: &str) -> String {
    match source_type {
        "claude-ai" => format!(
            "claude.ai conversation {}",
            string_at(&observation.payload, &["conversation_uuid"]).unwrap_or("unknown")
        ),
        "github-issue" | "github-pr" => string_at(&observation.payload, &["title"])
            .unwrap_or("GitHub item")
            .to_owned(),
        "github-comment" => format!(
            "GitHub comment in {}",
            string_at(&observation.payload, &["repo"]).unwrap_or("unknown")
        ),
        "github-commit" => format!(
            "GitHub commit {}",
            string_at(&observation.payload, &["sha"]).unwrap_or("unknown")
        ),
        "claude-code" | "codex" => format!(
            "{source_type} session {}",
            coding_session_id(&observation.payload).unwrap_or_else(|| "unknown".to_owned())
        ),
        "gmail" => string_at(&observation.payload, &["subject"])
            .unwrap_or("Gmail message")
            .to_owned(),
        "discord" => string_at(&observation.payload, &["channel_name"])
            .or_else(|| string_at(&observation.payload, &["guild_name"]))
            .unwrap_or("Discord message")
            .to_owned(),
        _ => title(observation),
    }
}

fn personal_location(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "claude-ai" => string_at(&observation.payload, &["sender"]).map(str::to_owned),
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            string_at(&observation.payload, &["object_type"]).map(str::to_owned)
        }
        "claude-code" | "codex" => coding_item_location(&observation.payload),
        "gmail" => string_at(&observation.payload, &["from"]).map(str::to_owned),
        "discord" => string_at(&observation.payload, &["author_name"]).map(str::to_owned),
        _ => None,
    }
}

fn coding_item_location(payload: &serde_json::Value) -> Option<String> {
    let item = payload.get("item")?;
    match string_at(item, &["kind"])? {
        "message" => string_at(item, &["role"]).map(str::to_owned),
        "tool_call" => string_at(item, &["tool_name"]).map(|tool_name| format!("tool:{tool_name}")),
        _ => None,
    }
}

fn personal_container(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            string_at(&observation.payload, &["repo"]).map(str::to_owned)
        }
        "claude-ai" => string_at(&observation.payload, &["conversation_uuid"]).map(str::to_owned),
        "claude-code" | "codex" => coding_session_id(&observation.payload),
        "gmail" => string_at(&observation.payload, &["account_id"]).map(str::to_owned),
        "discord" => string_at(&observation.payload, &["channel_id"]).map(str::to_owned),
        _ => string_at(&observation.payload, &["artifact", "containerId"]).map(str::to_owned),
    }
}

fn personal_thread_key(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "claude-ai" => string_at(&observation.payload, &["conversation_uuid"])
            .map(|conversation| format!("claude-ai:conversation:{conversation}")),
        "claude-code" | "codex" => {
            let session = coding_session_id(&observation.payload)?;
            let root =
                coding_parent_session_id(&observation.payload).unwrap_or_else(|| session.clone());
            Some(format!("{source_type}:session:{root}"))
        }
        "gmail" => string_at(&observation.payload, &["thread_id"])
            .map(|thread| format!("gmail:thread:{thread}")),
        "discord" => string_at(&observation.payload, &["referenced_message_id"])
            .or_else(|| string_at(&observation.payload, &["message_id"]))
            .map(|message| format!("discord:thread:{message}")),
        _ => None,
    }
}

fn personal_metadata(
    observation: &Observation,
    source_type: &str,
    thread_key: Option<&str>,
) -> serde_json::Value {
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "observation_id".to_owned(),
        serde_json::Value::String(observation.id.as_str().to_owned()),
    );
    metadata.insert(
        "schema".to_owned(),
        serde_json::Value::String(observation.schema.as_str().to_owned()),
    );
    metadata.insert(
        "source_type".to_owned(),
        serde_json::Value::String(source_type.to_owned()),
    );
    metadata.insert(
        "source_object_id".to_owned(),
        serde_json::Value::String(
            observation
                .meta
                .get("object_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(observation.id.as_str())
                .to_owned(),
        ),
    );
    let mut privacy_keys = vec![serde_json::Value::String(
        observation.subject.as_str().to_owned(),
    )];
    privacy_keys.extend(
        observation_identifiers(observation)
            .into_iter()
            .map(serde_json::Value::String),
    );
    metadata.insert(
        "privacy_keys".to_owned(),
        serde_json::Value::Array(privacy_keys),
    );
    if let Some(source_system) = &observation.source_system {
        metadata.insert(
            "source_system".to_owned(),
            serde_json::Value::String(source_system.as_str().to_owned()),
        );
    }
    if let Some(thread_key) = thread_key {
        metadata.insert(
            "thread_key".to_owned(),
            serde_json::Value::String(thread_key.to_owned()),
        );
    }
    if let Some(object_type) = string_at(&observation.payload, &["object_type"]) {
        metadata.insert(
            "object_type".to_owned(),
            serde_json::Value::String(object_type.to_owned()),
        );
    }
    if let Some(repo) = string_at(&observation.payload, &["repo"]) {
        metadata.insert(
            "repo".to_owned(),
            serde_json::Value::String(repo.to_owned()),
        );
    }
    if matches!(source_type, "gmail") {
        for field in [
            "account_id",
            "message_id",
            "thread_id",
            "from",
            "in_reply_to",
        ] {
            if let Some(value) = string_at(&observation.payload, &[field]) {
                metadata.insert(
                    field.to_owned(),
                    serde_json::Value::String(value.to_owned()),
                );
            }
        }
        if let Some(references) = observation.payload.get("references") {
            metadata.insert("references".to_owned(), references.clone());
        }
    }
    if matches!(source_type, "discord") {
        for field in [
            "channel_id",
            "message_id",
            "author_id",
            "guild_id",
            "referenced_message_id",
        ] {
            if let Some(value) = string_at(&observation.payload, &[field]) {
                metadata.insert(
                    field.to_owned(),
                    serde_json::Value::String(value.to_owned()),
                );
            }
        }
    }
    if matches!(source_type, "claude-code" | "codex") {
        if let Some(session_id) = coding_session_id(&observation.payload) {
            metadata.insert(
                "session_id".to_owned(),
                serde_json::Value::String(session_id),
            );
        }
        if let Some(parent_session_id) = coding_parent_session_id(&observation.payload) {
            metadata.insert(
                "parent_session_id".to_owned(),
                serde_json::Value::String(parent_session_id),
            );
        }
        metadata.insert(
            "is_sidechain".to_owned(),
            serde_json::Value::Bool(coding_is_sidechain(&observation.payload)),
        );
        if let Some(message_id) = coding_message_id(&observation.payload) {
            metadata.insert(
                "message_id".to_owned(),
                serde_json::Value::String(message_id),
            );
        }
        if let Some(parent_message_id) =
            string_owned_at(&observation.payload, &["parent_message_uuid"])
                .or_else(|| string_owned_at(&observation.payload, &["parent_message_id"]))
        {
            metadata.insert(
                "parent_message_id".to_owned(),
                serde_json::Value::String(parent_message_id),
            );
        }
    }
    serde_json::Value::Object(metadata)
}

fn number_like(payload: &serde_json::Value, field: &str) -> Option<String> {
    payload.get(field).and_then(|value| {
        value
            .as_i64()
            .map(|value| value.to_string())
            .or_else(|| value.as_str().map(str::to_owned))
    })
}

fn coding_session_id(payload: &serde_json::Value) -> Option<String> {
    string_owned_at(payload, &["session_id"])
}

fn coding_parent_session_id(payload: &serde_json::Value) -> Option<String> {
    string_owned_at(payload, &["parent_thread_id"])
}

fn coding_message_id(payload: &serde_json::Value) -> Option<String> {
    string_owned_at(payload, &["object_id"])
}

fn coding_is_sidechain(payload: &serde_json::Value) -> bool {
    bool_at(payload, &["is_sidechain"]).unwrap_or(false)
}

fn string_owned_at(value: &serde_json::Value, path: &[&str]) -> Option<String> {
    string_at(value, path).map(str::to_owned)
}

fn title(observation: &Observation) -> String {
    string_at(&observation.payload, &["title"])
        .unwrap_or("Untitled")
        .to_owned()
}

fn canonical_uri(observation: &Observation) -> String {
    string_at(&observation.payload, &["artifact", "canonicalUri"])
        .unwrap_or("")
        .to_owned()
}

pub fn linked_form_sheet_id(observation: &Observation) -> Option<String> {
    if observation.schema.as_str() == "schema:workspace-object-snapshot"
        && string_at(&observation.payload, &["artifact", "service"]) == Some("forms")
    {
        string_at(&observation.payload, &["metadata", "linkedSheetId"]).map(str::to_owned)
    } else {
        None
    }
}

fn source_object_id(observation: &Observation) -> Option<&str> {
    string_at(&observation.payload, &["artifact", "sourceObjectId"])
}

fn is_opted_out(opt_out: &HashSet<String>, payload: &serde_json::Value, paths: &[&str]) -> bool {
    paths.iter().any(|path| {
        let parts = path.split('.').collect::<Vec<_>>();
        string_at(payload, &parts).is_some_and(|value| opt_out.contains(value))
    })
}

fn string_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_str()
}

fn bool_at(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_bool()
}

fn parse_sharing_level(value: &str) -> Option<SharingThreshold> {
    match value {
        "specific-users" | "specific_users" => Some(SharingThreshold::SpecificUsers),
        "domain" => Some(SharingThreshold::Domain),
        "anyone-with-link" | "anyone_with_link" => Some(SharingThreshold::AnyoneWithLink),
        "public" => Some(SharingThreshold::Public),
        "private" => Some(SharingThreshold::SpecificUsers),
        _ => None,
    }
}

fn cells_to_text(row: &serde_json::Value) -> String {
    row.pointer("/cells")
        .and_then(serde_json::Value::as_array)
        .map(|cells| {
            cells
                .iter()
                .filter_map(|cell| {
                    let value = string_at(cell, &["value"])?;
                    let header = string_at(cell, &["header"]).unwrap_or("");
                    Some(if header.is_empty() {
                        value.to_owned()
                    } else {
                        format!("{header}: {value}")
                    })
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn value_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Array(values) => {
            values.iter().map(value_text).collect::<Vec<_>>().join("\n")
        }
        serde_json::Value::Object(map) => {
            map.values().map(value_text).collect::<Vec<_>>().join("\n")
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_core::domain::*;

    fn obs(schema: &str, payload: serde_json::Value) -> Observation {
        obs_with_source(schema, None, payload)
    }

    fn obs_with_source(
        schema: &str,
        source_system: Option<&str>,
        payload: serde_json::Value,
    ) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new(schema),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: source_system.map(SourceSystemRef::new),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("entity:test"),
            target: None,
            payload,
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("test"),
            meta: serde_json::json!({"canonical_json": "{}"}),
        }
    }

    fn consent(subject: &str, status: &str, published: &str) -> Observation {
        let mut observation = obs(
            "schema:consent-decision",
            serde_json::json!({"status": status, "identifier": "person-1"}),
        );
        observation.subject = EntityRef::new(subject);
        observation.published = published.parse().unwrap();
        observation.recorded_at = observation.published + chrono::Duration::seconds(1);
        observation
    }

    #[test]
    fn retraction_removes_prior_record_incrementally_and_keeps_canonical_target() {
        let projector = CorpusProjector::personal_all_text_config();
        let mut target = obs(
            "schema:claude-message",
            serde_json::json!({"text": "salvageable secret"}),
        );
        target.meta["object_id"] = serde_json::json!("claude:message:1");
        let mut retraction = obs(
            "schema:claude-message",
            serde_json::json!({"text": "retraction event"}),
        );
        retraction.meta["retracts"] = serde_json::json!({
            "source_object_id": "claude:message:1"
        });

        let mut state = CorpusProjectionState::default();
        assert_eq!(
            state.fold_observations(&projector, &[target.clone()]).len(),
            1
        );
        assert!(
            state
                .fold_observations(&projector, &[retraction])
                .is_empty()
        );
        assert_eq!(target.payload["text"], "salvageable secret");
    }

    #[test]
    fn consent_opt_out_is_record_scoped_and_validated_on_demand() {
        let projector = CorpusProjector::personal_all_text_config();
        let target = obs(
            "schema:claude-message",
            serde_json::json!({"text": "person secret"}),
        );
        let mut consent = obs(
            "schema:consent-decision",
            serde_json::json!({"status": "opted_out"}),
        );
        consent.subject = target.subject.clone();
        assert_eq!(
            projector
                .project_observations(std::slice::from_ref(&target))
                .len(),
            1
        );
        assert!(
            projector
                .project_observations(&[target.clone(), consent.clone()])
                .is_empty()
        );
        let observations = vec![target, consent];
        let report = projector
            .validate_on_demand_full(&observations)
            .expect("on-demand privacy validation should pass");
        assert_eq!(report.observations_checked, 2);
        assert_eq!(report.records_checked, 0);
    }

    #[test]
    fn consent_reconsent_restores_incrementally_and_late_old_decision_does_not_win() {
        let projector = CorpusProjector::personal_all_text_config();
        let mut target = obs(
            "schema:claude-message",
            serde_json::json!({"text": "restored secret", "email": "person-1"}),
        );
        target.subject = EntityRef::new("person:1");
        let opt_out = consent("person:1", "opted_out", "2026-01-02T00:00:00Z");
        let reconsent = consent("person:1", "unrestricted", "2026-01-03T00:00:00Z");
        let late_old_opt_out = consent("person:1", "opted_out", "2026-01-02T00:00:00Z");

        let mut state = CorpusProjectionState::default();
        assert_eq!(
            state.fold_observations(&projector, &[target.clone()]).len(),
            1
        );
        assert!(state.fold_observations(&projector, &[opt_out]).is_empty());
        assert_eq!(state.fold_observations(&projector, &[reconsent]).len(), 1);
        assert_eq!(
            state
                .fold_observations(&projector, &[late_old_opt_out])
                .len(),
            1
        );
    }

    #[test]
    fn retraction_remains_permanently_shielded_after_reconsent() {
        let projector = CorpusProjector::personal_all_text_config();
        let mut target = obs(
            "schema:claude-message",
            serde_json::json!({"text": "never restored"}),
        );
        target.subject = EntityRef::new("person:1");
        target.meta["object_id"] = serde_json::json!("claude:message:permanent");
        let mut retraction = obs(
            "schema:claude-message",
            serde_json::json!({"text": "retraction"}),
        );
        retraction.meta["retracts"] = serde_json::json!({
            "source_object_id": "claude:message:permanent"
        });
        let reconsent = consent("person:1", "unrestricted", "2026-01-03T00:00:00Z");

        let mut state = CorpusProjectionState::default();
        state.fold_observations(&projector, &[target, retraction]);
        assert!(state.fold_observations(&projector, &[reconsent]).is_empty());
    }

    #[test]
    fn slack_filter_rejects_non_matching_channels_and_bots() {
        let projector = CorpusProjector::default_config();
        let hidden = obs(
            "schema:slack-message",
            serde_json::json!({
                "channel_name": "general",
                "channel_id": "C1",
                "is_public_channel": true,
                "is_bot": false,
                "text": "hello",
                "ts": "1.000000"
            }),
        );
        let bot = obs(
            "schema:slack-message",
            serde_json::json!({
                "channel_name": "123_event",
                "channel_id": "C2",
                "is_public_channel": true,
                "is_bot": true,
                "text": "bot",
                "ts": "2.000000"
            }),
        );
        assert!(projector.project_observations(&[hidden, bot]).is_empty());
    }

    #[test]
    fn form_response_content_never_enters_corpus() {
        let projector = CorpusProjector::default_config();
        let content = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey",
                "artifact": {
                    "service": "forms",
                    "objectType": "form-response-content",
                    "canonicalUri": "https://forms"
                },
                "response": {"answers": {"secret": "x"}}
            }),
        );
        assert!(projector.project_observations(&[content]).is_empty());
    }

    fn drive_obs(file_id: &str, parent_id: &str, sharing_level: &str) -> Observation {
        obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Drive file",
                "artifact": {
                    "service": "drive",
                    "objectType": "file",
                    "sourceObjectId": file_id,
                    "canonicalUri": "https://drive/file"
                },
                "metadata": {
                    "parentIds": [parent_id],
                    "sharingLevel": sharing_level
                },
                "native": {"text": "drive text"}
            }),
        )
    }

    fn corpus_with_allowed_folder() -> CorpusProjector {
        CorpusProjector::new(CorpusConfig {
            allowed_folder_ids: ["folder-allowed".to_owned()].into_iter().collect(),
            ..CorpusConfig::default()
        })
    }

    #[test]
    fn drive_files_below_sharing_threshold_are_excluded() {
        let projector = corpus_with_allowed_folder();
        let private = drive_obs("file-private", "folder-allowed", "specific-users");
        let domain = drive_obs("file-domain", "folder-allowed", "domain");

        let records = projector.project_observations(&[private, domain]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "drive");
        assert_eq!(records[0].metadata["sharing_level"], "domain");
    }

    #[test]
    fn drive_files_are_denied_when_allowed_folders_is_empty() {
        let projector = CorpusProjector::default_config();
        let file = drive_obs("file-domain", "folder-allowed", "domain");

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn drive_files_outside_allowed_folders_are_excluded() {
        let projector = corpus_with_allowed_folder();
        let file = drive_obs("file-domain", "folder-other", "domain");

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn excluded_drive_file_ids_are_excluded() {
        let projector = CorpusProjector::new(CorpusConfig {
            allowed_folder_ids: ["folder-allowed".to_owned()].into_iter().collect(),
            excluded_file_ids: ["file-denied".to_owned()].into_iter().collect(),
            ..CorpusConfig::default()
        });
        let file = drive_obs("file-denied", "folder-allowed", "domain");

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn linked_form_response_sheets_are_excluded() {
        let projector = CorpusProjector::default_config();
        let form = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey",
                "artifact": {
                    "service": "forms",
                    "objectType": "form",
                    "canonicalUri": "https://forms/form"
                },
                "metadata": {"linkedSheetId": "sheet-1"}
            }),
        );
        let sheet = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey responses",
                "artifact": {
                    "service": "sheets",
                    "objectType": "spreadsheet",
                    "sourceObjectId": "sheet-1",
                    "canonicalUri": "https://sheets/sheet-1"
                },
                "native": {
                    "tabs": [{
                        "name": "Responses",
                        "rows": [{
                            "rowNumber": 2,
                            "cells": [{"header": "Email", "value": "a@example.test"}]
                        }]
                    }]
                }
            }),
        );

        let records = projector.project_observations(&[form, sheet]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "forms");
    }

    #[test]
    fn direct_form_response_sheet_metadata_is_excluded() {
        let projector = CorpusProjector::default_config();
        let sheet = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey responses",
                "artifact": {
                    "service": "sheets",
                    "objectType": "spreadsheet",
                    "canonicalUri": "https://sheets/sheet-1"
                },
                "metadata": {"formResponseSheet": true},
                "native": {
                    "tabs": [{
                        "name": "Responses",
                        "rows": [{
                            "rowNumber": 2,
                            "cells": [{"header": "Email", "value": "a@example.test"}]
                        }]
                    }]
                }
            }),
        );

        assert!(projector.project_observations(&[sheet]).is_empty());
    }

    #[test]
    fn opted_out_drive_owner_is_excluded() {
        let projector = CorpusProjector::new(CorpusConfig {
            allowed_folder_ids: ["folder-allowed".to_owned()].into_iter().collect(),
            opt_out_people: ["owner@example.test".to_owned()].into_iter().collect(),
            ..CorpusConfig::default()
        });
        let mut file = drive_obs("file-domain", "folder-allowed", "domain");
        file.payload["relations"] = serde_json::json!({"owner": "owner@example.test"});

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn docs_chunks_create_heading_records() {
        let projector = CorpusProjector::default_config();
        let doc = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Planning Doc",
                "artifact": {
                    "service": "docs",
                    "objectType": "document",
                    "canonicalUri": "https://docs/doc-1",
                    "containerId": "folder-1"
                },
                "native": {
                    "chunks": [
                        {"heading": "Intro", "anchor": "h.intro", "text": "Alpha"},
                        {"heading": "Plan", "text": "Beta"}
                    ]
                }
            }),
        );

        let records = projector.project_observations(&[doc]);

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source_location.as_deref(), Some("Intro"));
        assert_eq!(records[0].anchor_url, "https://docs/doc-1#h.intro");
        assert_eq!(records[1].source_location.as_deref(), Some("Plan"));
        assert_eq!(records[1].text, "Beta");
    }

    #[test]
    fn sheets_rows_create_row_records() {
        let projector = CorpusProjector::default_config();
        let sheet = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Inventory",
                "artifact": {
                    "service": "sheets",
                    "objectType": "spreadsheet",
                    "canonicalUri": "https://sheets/sheet-1"
                },
                "native": {
                    "tabs": [{
                        "name": "Items",
                        "rows": [
                            {
                                "rowNumber": 2,
                                "cells": [
                                    {"header": "Name", "value": "Cable"},
                                    {"header": "Count", "value": "3"}
                                ]
                            },
                            {"rowNumber": 3, "cells": [{}]}
                        ]
                    }]
                }
            }),
        );

        let records = projector.project_observations(&[sheet]);

        assert_eq!(records.len(), 1);
        assert!(records[0].record_id.ends_with(":Items:2"));
        assert_eq!(records[0].source_type, "sheets");
        assert_eq!(records[0].source_location.as_deref(), Some("Items row 2"));
        assert_eq!(records[0].text, "Name: Cable\nCount: 3");
    }

    #[test]
    fn bot_answer_log_schema_never_enters_corpus() {
        let projector = CorpusProjector::default_config();
        let answer = obs(
            "schema:bot-answer-log",
            serde_json::json!({
                "title": "Answer",
                "artifact": {
                    "service": "docs",
                    "objectType": "document",
                    "canonicalUri": "https://docs/answer"
                },
                "native": {"chunks": [{"text": "should stay out"}]}
            }),
        );

        assert!(projector.project_observations(&[answer]).is_empty());
    }

    #[test]
    fn personal_all_text_indexes_personal_lake_source_types() {
        let projector = CorpusProjector::personal_all_text_config();
        let records = projector.project_observations(&[
            obs_with_source(
                "schema:claude-message",
                Some("sys:claude-ai"),
                serde_json::json!({
                    "conversation_uuid": "conv-1",
                    "message_uuid": "msg-1",
                    "sender": "human",
                    "text": "needle claude ai"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "issue",
                    "repo": "owner/repo",
                    "number": 1,
                    "title": "needle issue",
                    "body": "body"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "pull_request",
                    "repo": "owner/repo",
                    "number": 2,
                    "title": "needle pr",
                    "body": "body"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "issue_comment",
                    "repo": "owner/repo",
                    "id": 3,
                    "body": "needle comment"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "commit",
                    "repo": "owner/repo",
                    "sha": "abc",
                    "message": "needle commit"
                }),
            ),
            obs_with_source(
                "schema:coding-agent-message",
                Some("sys:claude-code"),
                serde_json::json!({
                    "session_id": "main-session",
                    "transcript_id": "main-transcript",
                    "parent_message_id": null,
                    "is_sidechain": false,
                    "parent_thread_id": null,
                    "thread_source": "main",
                    "object_id": "cc-1",
                    "item": {
                        "kind": "message",
                        "role": "assistant",
                        "text": "needle claude code"
                    }
                }),
            ),
            obs_with_source(
                "schema:coding-agent-message",
                Some("sys:codex"),
                serde_json::json!({
                    "session_id": "codex-session",
                    "transcript_id": "codex-transcript",
                    "parent_message_id": null,
                    "is_sidechain": false,
                    "parent_thread_id": null,
                    "thread_source": "main",
                    "object_id": "codex-1",
                    "item": {
                        "kind": "message",
                        "role": "assistant",
                        "text": "needle codex"
                    }
                }),
            ),
            obs_with_source(
                "schema:gmail-message",
                Some("sys:gmail"),
                serde_json::json!({
                    "account_id": "me@example.test",
                    "message_id": "<m1@example.test>",
                    "thread_id": "thread-1",
                    "from": "sender@example.test",
                    "subject": "needle gmail subject",
                    "text": "needle gmail body",
                    "references": ["<root@example.test>"]
                }),
            ),
            obs_with_source(
                "schema:discord-message",
                Some("sys:discord"),
                serde_json::json!({
                    "channel_id": "D01",
                    "message_id": "M01",
                    "author_id": "U01",
                    "author_name": "Alice",
                    "content": "needle discord",
                    "is_dm": true
                }),
            ),
        ]);

        let source_types = records
            .iter()
            .map(|record| record.source_type.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            source_types,
            std::collections::BTreeSet::from([
                "claude-ai",
                "github-issue",
                "github-pr",
                "github-comment",
                "github-commit",
                "claude-code",
                "codex",
                "discord",
                "gmail",
            ])
        );
    }

    #[test]
    fn personal_all_text_preserves_gmail_thread_metadata() {
        let projector = CorpusProjector::personal_all_text_config();
        let records = projector.project_observations(&[obs_with_source(
            "schema:gmail-message",
            Some("sys:gmail"),
            serde_json::json!({
                "account_id": "me@example.test",
                "message_id": "<reply@example.test>",
                "thread_id": "thread-1",
                "from": "sender@example.test",
                "subject": "Re: Plan",
                "text": "reply body",
                "references": ["<root@example.test>"],
                "in_reply_to": "<root@example.test>"
            }),
        )]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "gmail");
        assert_eq!(
            records[0].thread_ts.as_deref(),
            Some("gmail:thread:thread-1")
        );
        assert_eq!(records[0].metadata["message_id"], "<reply@example.test>");
        assert_eq!(records[0].metadata["references"][0], "<root@example.test>");
    }

    #[test]
    fn personal_all_text_preserves_coding_agent_sidechain_metadata() {
        let projector = CorpusProjector::personal_all_text_config();
        let records = projector.project_observations(&[obs_with_source(
            "schema:coding-agent-message",
            Some("sys:claude-code"),
            serde_json::json!({
                "session_id": "child-session",
                "transcript_id": "child-transcript",
                "parent_message_id": null,
                "parent_session_id": "main-session",
                "is_sidechain": true,
                "parent_thread_id": "main-session",
                "thread_source": "sidechain",
                "object_id": "child-message",
                "item": {
                    "kind": "message",
                    "role": "assistant",
                    "text": "sidechain finding"
                }
            }),
        )]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "claude-code");
        assert_eq!(records[0].source_location.as_deref(), Some("assistant"));
        assert_eq!(
            records[0].metadata["thread_key"],
            "claude-code:session:main-session"
        );
        assert_eq!(records[0].metadata["session_id"], "child-session");
        assert_eq!(records[0].metadata["parent_session_id"], "main-session");
        assert_eq!(records[0].metadata["is_sidechain"], true);
        assert_eq!(records[0].metadata["message_id"], "child-message");
    }

    #[test]
    fn personal_all_text_indexes_coding_agent_tool_call_references() {
        let projector = CorpusProjector::personal_all_text_config();
        let records = projector.project_observations(&[obs_with_source(
            "schema:coding-agent-message",
            Some("sys:codex"),
            serde_json::json!({
                "session_id": "codex-session",
                "transcript_id": "codex-transcript",
                "parent_message_id": null,
                "is_sidechain": false,
                "parent_thread_id": null,
                "thread_source": "main",
                "object_id": "codex-tool-1",
                "item": {
                    "kind": "tool_call",
                    "tool_name": "Read",
                    "references": {
                        "file_path": "D:/repo/needle.md",
                        "pattern": "needle pattern"
                    }
                }
            }),
        )]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "codex");
        assert_eq!(records[0].source_location.as_deref(), Some("tool:Read"));
        assert!(records[0].text.contains("D:/repo/needle.md"));
        assert!(records[0].text.contains("needle pattern"));
    }

    #[test]
    fn nfkc_normalizes_fullwidth_digits() {
        assert_eq!(normalized_text("１２３"), "123");
    }
}
