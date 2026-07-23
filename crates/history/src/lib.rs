use std::collections::{BTreeMap, BTreeSet};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, Utc};
use lethe_adapter_coding_agent::{BackboneHistoryRecord, BackboneItem};
use lethe_core::domain::{
    AuthorityModel, BlobRef, CaptureModel, DataSpaceId, EntityRef, IdempotencyKey, Observation,
    ObservationId, ObserverRef, OperationalEventId, SchemaRef, SemVer, SourceSystemRef,
};
use lethe_storage_api::{
    ObservationStore, OperationalAppendOutcome, OperationalAppendRequest, OperationalEvent,
    OperationalStoragePorts, StorageError, StoredObservation,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const HISTORY_MESSAGE_EVENT_TYPE: &str = "history.message_imported";
pub const HISTORY_IMPORT_EVENT_TYPE: &str = "history.import_completed";
pub const HISTORY_SCHEMA: &str = "schema:history-message";
pub const HISTORY_IMPORT_SCHEMA: &str = "schema:history-import-receipt";
pub const HISTORY_SCHEMA_VERSION: &str = "1.0.0";
pub const HISTORY_ACTIVATION_HANDOFF_SCHEMA: &str = "schema:history-activation-handoff";
pub const HISTORY_ACTIVATION_HANDOFF_VERSION: &str = "1.0.0";
pub const HISTORY_SOURCE_INSTANCE_META: &str = "source_instance";
pub const HISTORY_OBJECT_ID_META: &str = "object_id";
pub const UPSTREAM_SOURCE_KIND_META: &str = "upstream_source_kind";
pub const UPSTREAM_SOURCE_INSTANCE_META: &str = "upstream_source_instance_id";
pub const UPSTREAM_SESSION_META: &str = "upstream_session_id";
pub const UPSTREAM_MESSAGE_META: &str = "upstream_message_id";

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("history invariant violation: {0}")]
    Invariant(String),
    #[error("history manifest mismatch: expected {expected}, actual {actual}")]
    ManifestMismatch { expected: String, actual: String },
    #[error("history ownership is unresolved for: {0}")]
    UnresolvedOwnership(String),
    #[error("history source identity collision for {0}")]
    SourceIdentityCollision(String),
    #[error("history sources contain {0} cross-source native identity overlaps")]
    CrossSourceOverlap(u64),
    #[error("history reference not found: {0}")]
    NotFound(String),
    #[error("history query cursor is invalid: {0}")]
    InvalidCursor(String),
    #[error(
        "history query cursor is stale: cursor source {cursor_source}, current source {current_source}"
    )]
    CursorStale {
        cursor_source: String,
        current_source: String,
    },
    #[error(
        "history query result exceeds max_result_bytes: required {required}, maximum {maximum}"
    )]
    ResultTooLarge { required: usize, maximum: usize },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type HistoryResult<T> = Result<T, HistoryError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistorySourceKind {
    ClaudeCode,
    ClaudeAi,
    Codex,
    Intercom,
    Lethe,
    NaniholdLegacy,
    SystemSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OwnershipAssignment {
    Personal { owner_id: String },
    Unresolved { reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitmentStatus {
    Open,
    Fulfilled,
    Cancelled,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HistoryRecordKind {
    Message,
    Decision {
        decision_id: String,
        #[serde(default)]
        supersedes: Vec<String>,
    },
    Commitment {
        commitment_id: String,
        status: CommitmentStatus,
    },
    WorkItem {
        work_item_id: String,
        state: String,
    },
    Preference {
        preference_key: String,
    },
    CurrentState {
        state_key: String,
    },
    NodeMemory {
        memory_id: String,
        node_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryRawRecord {
    pub source_session_id: String,
    pub source_message_id: String,
    pub parent_message_id: Option<String>,
    pub published_at: DateTime<Utc>,
    pub ordinal: u64,
    pub author: String,
    pub surface: String,
    pub channel: String,
    pub text: String,
    pub record_kind: HistoryRecordKind,
    pub raw: Vec<u8>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySourceInventory {
    pub source_kind: HistorySourceKind,
    pub source_instance_id: String,
    pub cutover_cursor: String,
    pub ownership: OwnershipAssignment,
    pub records: Vec<HistoryRawRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredHistorySource {
    pub source_kind: HistorySourceKind,
    pub source_instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryInventoryRequest {
    pub inventory_id: String,
    pub data_space_id: DataSpaceId,
    pub captured_at: DateTime<Utc>,
    pub required_sources: Vec<RequiredHistorySource>,
    pub sources: Vec<HistorySourceInventory>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryManifestEntry {
    pub source_session_id: String,
    pub source_message_id: String,
    pub parent_message_id: Option<String>,
    pub published_at: DateTime<Utc>,
    pub ordinal: u64,
    pub author: String,
    pub surface: String,
    pub channel: String,
    pub text: String,
    pub record_kind: HistoryRecordKind,
    pub raw_sha256: String,
    pub raw_bytes: u64,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySourceManifest {
    pub source_kind: HistorySourceKind,
    pub source_instance_id: String,
    pub cutover_cursor: String,
    pub ownership: OwnershipAssignment,
    pub records: Vec<HistoryManifestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryImportManifest {
    pub inventory_id: String,
    pub data_space_id: DataSpaceId,
    pub captured_at: DateTime<Utc>,
    pub required_sources: Vec<RequiredHistorySource>,
    pub sources: Vec<HistorySourceManifest>,
    pub cross_source_overlap_identities: u64,
    pub manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryInventoryReport {
    pub manifest: HistoryImportManifest,
    pub source_records: u64,
    pub unique_records: u64,
    pub duplicate_source_records: u64,
    pub cross_source_overlap_identities: u64,
    pub raw_bytes: u64,
    pub unresolved_sources: Vec<String>,
    pub ready_for_import: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryImportCommand {
    pub inventory: HistoryInventoryRequest,
    pub expected_manifest_digest: String,
    pub admission_generations: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryImportReceipt {
    pub receipt_id: String,
    pub inventory_id: String,
    pub data_space_id: DataSpaceId,
    pub manifest_digest: String,
    pub captured_at: DateTime<Utc>,
    pub source_count: u64,
    pub message_count: u64,
    pub raw_bytes: u64,
    pub cross_source_overlap_identities: u64,
    pub cutover_cursors: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryActivationSource {
    pub source_id: String,
    pub source_kind: HistorySourceKind,
    pub ownership: String,
    pub owner_id: Option<String>,
    pub record_count: u64,
    pub raw_bytes: u64,
    pub digest_sha256: String,
    pub cutover_cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryActivationSessionEntry {
    pub session_ref: String,
    pub source_kind: HistorySourceKind,
    pub source_id: String,
    pub source_session_id: String,
    pub first_message_at: DateTime<Utc>,
    pub last_message_at: DateTime<Utc>,
    pub message_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryActivationHandoff {
    pub schema: String,
    pub schema_version: String,
    pub inventory_id: String,
    pub data_space_id: DataSpaceId,
    pub manifest_digest: String,
    pub record_count: u64,
    pub raw_bytes: u64,
    pub cross_source_overlap_identities: u64,
    pub sources: Vec<HistoryActivationSource>,
    pub session_count: u64,
    pub sessions: Vec<HistoryActivationSessionEntry>,
    pub session_index_ref: String,
    pub open_commitments_ref: String,
    pub current_state_ref: String,
}

impl HistoryActivationHandoff {
    pub fn validate(&self) -> HistoryResult<()> {
        if self.schema != HISTORY_ACTIVATION_HANDOFF_SCHEMA
            || self.schema_version != HISTORY_ACTIVATION_HANDOFF_VERSION
        {
            return Err(HistoryError::Invariant(
                "history activation handoff schema or version mismatch".to_owned(),
            ));
        }
        validate_non_blank("handoff inventory_id", &self.inventory_id)?;
        validate_non_blank("handoff data_space_id", self.data_space_id.as_str())?;
        validate_sha256("handoff manifest_digest", &self.manifest_digest)?;
        let mut source_ids = BTreeSet::new();
        let source_records = self.sources.iter().try_fold(0_u64, |total, source| {
            validate_non_blank("handoff source_id", &source.source_id)?;
            let Some((kind, instance)) = source.source_id.split_once(':') else {
                return Err(HistoryError::Invariant(
                    "activation handoff source_id is malformed or duplicated".to_owned(),
                ));
            };
            if kind != source_kind_name(source.source_kind)
                || instance.trim().is_empty()
                || !source_ids.insert(source.source_id.clone())
            {
                return Err(HistoryError::Invariant(
                    "activation handoff source_id is malformed or duplicated".to_owned(),
                ));
            }
            if source.ownership != "personal" {
                return Err(HistoryError::Invariant(
                    "activation handoff source must have resolved personal ownership".to_owned(),
                ));
            }
            validate_non_blank(
                "handoff owner_id",
                source.owner_id.as_deref().unwrap_or_default(),
            )?;
            validate_non_blank("handoff cutover_cursor", &source.cutover_cursor)?;
            validate_sha256("handoff digest_sha256", &source.digest_sha256)?;
            total.checked_add(source.record_count).ok_or_else(|| {
                HistoryError::Invariant("handoff source record count overflow".to_owned())
            })
        })?;
        let source_bytes = self.sources.iter().try_fold(0_u64, |total, source| {
            total.checked_add(source.raw_bytes).ok_or_else(|| {
                HistoryError::Invariant("handoff source byte count overflow".to_owned())
            })
        })?;
        if source_records != self.record_count || source_bytes != self.raw_bytes {
            return Err(HistoryError::Invariant(
                "handoff source totals do not match manifest totals".to_owned(),
            ));
        }
        if self.session_count
            != u64::try_from(self.sessions.len()).map_err(|_| {
                HistoryError::Invariant("handoff session count does not fit u64".to_owned())
            })?
        {
            return Err(HistoryError::Invariant(
                "handoff session_count does not match entries".to_owned(),
            ));
        }
        validate_projection_ref("session_index_ref", &self.session_index_ref)?;
        validate_projection_ref("open_commitments_ref", &self.open_commitments_ref)?;
        validate_projection_ref("current_state_ref", &self.current_state_ref)?;
        for session in &self.sessions {
            validate_non_blank("handoff session_ref", &session.session_ref)?;
            validate_non_blank("handoff session source_id", &session.source_id)?;
            if !source_ids.contains(&session.source_id) {
                return Err(HistoryError::Invariant(
                    "handoff session references an unknown source_id".to_owned(),
                ));
            }
            validate_non_blank("handoff source_session_id", &session.source_session_id)?;
            if session.message_count == 0 || session.first_message_at > session.last_message_at {
                return Err(HistoryError::Invariant(
                    "handoff session has invalid count or time range".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryImportResult {
    pub receipt: HistoryImportReceipt,
    pub appended_messages: u64,
    pub duplicate_messages: u64,
    pub receipt_cursor: u64,
    pub receipt_was_duplicate: bool,
}

pub struct HistoryManifestDigestBuilder {
    hasher: Sha256,
    last_source_key: Option<String>,
    current_source_key: Option<String>,
    last_entry_key: Option<String>,
}

impl HistoryManifestDigestBuilder {
    pub fn new(
        inventory_id: &str,
        data_space_id: &DataSpaceId,
        captured_at: DateTime<Utc>,
        required_sources: &[RequiredHistorySource],
        cross_source_overlap_identities: u64,
    ) -> HistoryResult<Self> {
        validate_non_blank("inventory_id", inventory_id)?;
        validate_non_blank("data_space_id", data_space_id.as_str())?;
        let mut required_sources = required_sources.to_vec();
        required_sources.sort_by(source_key_cmp);
        reject_duplicate_required_sources(&required_sources)?;
        let mut hasher = Sha256::new();
        hasher.update(b"lethe-history-manifest-v3\0");
        update_framed_json(
            &mut hasher,
            &serde_json::json!({
                "inventory_id": inventory_id,
                "data_space_id": data_space_id,
                "captured_at": captured_at,
                "required_sources": required_sources,
                "cross_source_overlap_identities": cross_source_overlap_identities,
            }),
        )?;
        Ok(Self {
            hasher,
            last_source_key: None,
            current_source_key: None,
            last_entry_key: None,
        })
    }

    pub fn push_source(&mut self, source: &HistorySourceManifest) -> HistoryResult<()> {
        validate_non_blank("source_instance_id", &source.source_instance_id)?;
        validate_non_blank("cutover_cursor", &source.cutover_cursor)?;
        let key = source_key(source.source_kind, &source.source_instance_id);
        if self
            .last_source_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(HistoryError::Invariant(format!(
                "manifest sources are not in strict canonical order at {key}"
            )));
        }
        update_framed_json(
            &mut self.hasher,
            &serde_json::json!({
                "source_kind": source.source_kind,
                "source_instance_id": source.source_instance_id,
                "cutover_cursor": source.cutover_cursor,
                "ownership": source.ownership,
            }),
        )?;
        self.last_source_key = Some(key.clone());
        self.current_source_key = Some(key);
        self.last_entry_key = None;
        Ok(())
    }

    pub fn push_entry(&mut self, entry: &HistoryManifestEntry) -> HistoryResult<()> {
        if self.current_source_key.is_none() {
            return Err(HistoryError::Invariant(
                "manifest entry appeared before a source".to_owned(),
            ));
        }
        let key = manifest_entry_key(entry);
        if self
            .last_entry_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(HistoryError::Invariant(format!(
                "manifest entries are not in strict canonical order at {key}"
            )));
        }
        update_framed_json(&mut self.hasher, entry)?;
        self.last_entry_key = Some(key);
        Ok(())
    }

    pub fn finish(self) -> String {
        hex::encode(self.hasher.finalize())
    }
}

pub fn inventory_history(
    request: &HistoryInventoryRequest,
) -> HistoryResult<HistoryInventoryReport> {
    validate_non_blank("inventory_id", &request.inventory_id)?;
    validate_non_blank("data_space_id", request.data_space_id.as_str())?;
    if request.required_sources.is_empty() {
        return Err(HistoryError::Invariant(
            "required_sources must not be empty".to_owned(),
        ));
    }
    if request.sources.is_empty() {
        return Err(HistoryError::Invariant(
            "sources must not be empty".to_owned(),
        ));
    }

    let mut required_sources = request.required_sources.clone();
    required_sources.sort_by(source_key_cmp);
    reject_duplicate_required_sources(&required_sources)?;

    let mut source_records = 0_u64;
    let mut unique_records = 0_u64;
    let mut duplicate_source_records = 0_u64;
    let mut raw_bytes = 0_u64;
    let mut source_manifests = Vec::with_capacity(request.sources.len());
    let mut seen_sources = BTreeSet::new();
    let mut unresolved_sources = Vec::new();
    let mut provenance_sources = BTreeMap::<String, BTreeSet<String>>::new();

    for source in &request.sources {
        validate_source(source)?;
        let key = source_key(source.source_kind, &source.source_instance_id);
        if !seen_sources.insert(key.clone()) {
            return Err(HistoryError::Invariant(format!(
                "duplicate history source {key}"
            )));
        }
        if let OwnershipAssignment::Unresolved { reason } = &source.ownership {
            unresolved_sources.push(format!("{key}: {reason}"));
        }

        let mut unique = BTreeMap::<String, (&HistoryRawRecord, String)>::new();
        for record in &source.records {
            source_records = source_records.checked_add(1).ok_or_else(|| {
                HistoryError::Invariant("source record count overflow".to_owned())
            })?;
            validate_record(record)?;
            if let Some(provenance) = history_upstream_identity(record)? {
                provenance_sources
                    .entry(provenance)
                    .or_default()
                    .insert(key.clone());
            }
            let identity = source_record_identity(
                &source.source_instance_id,
                &record.source_session_id,
                &record.source_message_id,
            );
            let raw_sha256 = sha256_hex(&record.raw);
            if let Some((_, existing_digest)) = unique.get(&identity) {
                if existing_digest != &raw_sha256 {
                    return Err(HistoryError::SourceIdentityCollision(identity));
                }
                duplicate_source_records =
                    duplicate_source_records.checked_add(1).ok_or_else(|| {
                        HistoryError::Invariant("duplicate record count overflow".to_owned())
                    })?;
                continue;
            }
            unique.insert(identity, (record, raw_sha256));
        }

        let mut entries = unique
            .into_values()
            .map(|(record, raw_sha256)| {
                let entry = manifest_entry_for_raw_record(record)?;
                let bytes = entry.raw_bytes;
                raw_bytes = raw_bytes
                    .checked_add(bytes)
                    .ok_or_else(|| HistoryError::Invariant("raw byte count overflow".to_owned()))?;
                unique_records = unique_records.checked_add(1).ok_or_else(|| {
                    HistoryError::Invariant("unique record count overflow".to_owned())
                })?;
                if entry.raw_sha256 != raw_sha256 {
                    return Err(HistoryError::Invariant(
                        "raw digest changed while building manifest".to_owned(),
                    ));
                }
                Ok(entry)
            })
            .collect::<HistoryResult<Vec<_>>>()?;
        entries.sort_by(manifest_entry_cmp);
        source_manifests.push(HistorySourceManifest {
            source_kind: source.source_kind,
            source_instance_id: source.source_instance_id.clone(),
            cutover_cursor: source.cutover_cursor.clone(),
            ownership: source.ownership.clone(),
            records: entries,
        });
    }
    source_manifests.sort_by(|left, right| {
        source_key(left.source_kind, &left.source_instance_id)
            .cmp(&source_key(right.source_kind, &right.source_instance_id))
    });

    for required in &required_sources {
        let key = source_key(required.source_kind, &required.source_instance_id);
        if !seen_sources.contains(&key) {
            return Err(HistoryError::Invariant(format!(
                "required history source is missing: {key}"
            )));
        }
    }
    let cross_source_overlap_identities = u64::try_from(
        provenance_sources
            .values()
            .filter(|sources| sources.len() > 1)
            .count(),
    )
    .map_err(|_| {
        HistoryError::Invariant("cross-source overlap count does not fit u64".to_owned())
    })?;

    let mut digest_builder = HistoryManifestDigestBuilder::new(
        &request.inventory_id,
        &request.data_space_id,
        request.captured_at,
        &required_sources,
        cross_source_overlap_identities,
    )?;
    for source in &source_manifests {
        digest_builder.push_source(source)?;
        for entry in &source.records {
            digest_builder.push_entry(entry)?;
        }
    }
    let manifest_digest = digest_builder.finish();
    let manifest = HistoryImportManifest {
        inventory_id: request.inventory_id.clone(),
        data_space_id: request.data_space_id.clone(),
        captured_at: request.captured_at,
        required_sources,
        sources: source_manifests,
        cross_source_overlap_identities,
        manifest_digest,
    };
    unresolved_sources.sort();
    Ok(HistoryInventoryReport {
        manifest,
        source_records,
        unique_records,
        duplicate_source_records,
        cross_source_overlap_identities,
        raw_bytes,
        ready_for_import: unresolved_sources.is_empty() && cross_source_overlap_identities == 0,
        unresolved_sources,
    })
}

pub fn manifest_entry_for_raw_record(
    record: &HistoryRawRecord,
) -> HistoryResult<HistoryManifestEntry> {
    validate_record(record)?;
    let raw_bytes = u64::try_from(record.raw.len())
        .map_err(|_| HistoryError::Invariant("raw record length does not fit u64".to_owned()))?;
    Ok(HistoryManifestEntry {
        source_session_id: record.source_session_id.clone(),
        source_message_id: record.source_message_id.clone(),
        parent_message_id: record.parent_message_id.clone(),
        published_at: record.published_at,
        ordinal: record.ordinal,
        author: record.author.clone(),
        surface: record.surface.clone(),
        channel: record.channel.clone(),
        text: record.text.clone(),
        record_kind: record.record_kind.clone(),
        raw_sha256: sha256_hex(&record.raw),
        raw_bytes,
        metadata: record.metadata.clone(),
    })
}

pub fn import_history<T: OperationalStoragePorts + ?Sized>(
    store: &T,
    command: &HistoryImportCommand,
    max_blob_bytes: usize,
) -> HistoryResult<HistoryImportResult> {
    if max_blob_bytes == 0 {
        return Err(HistoryError::Invariant(
            "max_blob_bytes must be positive".to_owned(),
        ));
    }
    if store.data_space_id() != &command.inventory.data_space_id {
        return Err(HistoryError::Invariant(format!(
            "history manifest data space {} does not match Lake {}",
            command.inventory.data_space_id,
            store.data_space_id()
        )));
    }
    let report = inventory_history(&command.inventory)?;
    if report.manifest.manifest_digest != command.expected_manifest_digest {
        return Err(HistoryError::ManifestMismatch {
            expected: command.expected_manifest_digest.clone(),
            actual: report.manifest.manifest_digest,
        });
    }
    if !report.ready_for_import {
        if report.cross_source_overlap_identities > 0 {
            return Err(HistoryError::CrossSourceOverlap(
                report.cross_source_overlap_identities,
            ));
        }
        return Err(HistoryError::UnresolvedOwnership(
            report.unresolved_sources.join(", "),
        ));
    }

    let raw_by_identity = raw_record_map(&command.inventory)?;
    for (source_instance_id, generation) in &command.admission_generations {
        if *generation == 0 {
            return Err(HistoryError::Invariant(format!(
                "admission generation for source_instance_id {source_instance_id} must be positive"
            )));
        }
        if !report
            .manifest
            .sources
            .iter()
            .any(|source| &source.source_instance_id == source_instance_id)
        {
            return Err(HistoryError::Invariant(format!(
                "admission generation was supplied for source_instance_id {source_instance_id}, which is not in the import manifest"
            )));
        }
    }
    let mut requests_by_source = Vec::with_capacity(report.manifest.sources.len());
    for source in &report.manifest.sources {
        let owner_id = match &source.ownership {
            OwnershipAssignment::Personal { owner_id } => owner_id,
            OwnershipAssignment::Unresolved { .. } => unreachable!("guarded above"),
        };
        let mut requests = Vec::with_capacity(source.records.len());
        for entry in &source.records {
            let identity = source_record_identity(
                &source.source_instance_id,
                &entry.source_session_id,
                &entry.source_message_id,
            );
            let raw = raw_by_identity.get(&identity).ok_or_else(|| {
                HistoryError::Invariant(format!("manifest raw record is missing: {identity}"))
            })?;
            if sha256_hex(raw) != entry.raw_sha256 {
                return Err(HistoryError::ManifestMismatch {
                    expected: entry.raw_sha256.clone(),
                    actual: sha256_hex(raw),
                });
            }
            let blob_ref = store.put_blob(raw, max_blob_bytes)?;
            let expected_blob_ref = format!("blob:sha256:{}", entry.raw_sha256);
            if blob_ref.as_str() != expected_blob_ref {
                return Err(HistoryError::Invariant(format!(
                    "blob store returned {} for expected {}",
                    blob_ref, expected_blob_ref
                )));
            }
            requests.push(prepare_history_message_request(
                store.data_space_id(),
                source,
                owner_id,
                entry,
                blob_ref,
            )?);
        }
        if !requests.is_empty() {
            requests_by_source.push((
                source.source_instance_id.clone(),
                command
                    .admission_generations
                    .get(&source.source_instance_id)
                    .copied(),
                requests,
            ));
        }
    }

    let receipt = receipt_for(&report)?;
    let mut appended_messages = 0_u64;
    let mut duplicate_messages = 0_u64;
    for (source_instance_id, generation, requests) in requests_by_source {
        let outcomes = store.append_operational_events_v2_with_bridge(
            &source_instance_id,
            generation,
            &requests,
        )?;
        if outcomes.len() != requests.len() {
            return Err(HistoryError::Invariant(format!(
                "operational store returned {} outcomes for {} requests",
                outcomes.len(),
                requests.len()
            )));
        }
        for outcome in outcomes {
            match outcome {
                OperationalAppendOutcome::Appended { .. } => appended_messages += 1,
                OperationalAppendOutcome::Duplicate { .. } => duplicate_messages += 1,
                OperationalAppendOutcome::VersionConflict { expected, actual } => {
                    return Err(HistoryError::Invariant(format!(
                        "unexpected history message stream conflict: expected {expected}, actual {actual}"
                    )));
                }
            }
        }
    }
    let receipt_request = prepare_history_receipt_request(store.data_space_id(), &receipt)?;
    let receipt_outcome = store.append_operational_event(&receipt_request)?;
    let (receipt_cursor, receipt_was_duplicate) = match receipt_outcome {
        OperationalAppendOutcome::Appended { cursor, .. } => (cursor, false),
        OperationalAppendOutcome::Duplicate { cursor, .. } => (cursor, true),
        OperationalAppendOutcome::VersionConflict { expected, actual } => {
            return Err(HistoryError::Invariant(format!(
                "unexpected history receipt conflict: expected {expected}, actual {actual}"
            )));
        }
    };
    Ok(HistoryImportResult {
        receipt,
        appended_messages,
        duplicate_messages,
        receipt_cursor,
        receipt_was_duplicate,
    })
}

fn receipt_for(report: &HistoryInventoryReport) -> HistoryResult<HistoryImportReceipt> {
    let source_count = u64::try_from(report.manifest.sources.len())
        .map_err(|_| HistoryError::Invariant("source count does not fit u64".to_owned()))?;
    let cutover_cursors = report
        .manifest
        .sources
        .iter()
        .map(|source| {
            (
                source_key(source.source_kind, &source.source_instance_id),
                source.cutover_cursor.clone(),
            )
        })
        .collect();
    Ok(HistoryImportReceipt {
        receipt_id: format!("history-receipt:{}", report.manifest.manifest_digest),
        inventory_id: report.manifest.inventory_id.clone(),
        data_space_id: report.manifest.data_space_id.clone(),
        manifest_digest: report.manifest.manifest_digest.clone(),
        captured_at: report.manifest.captured_at,
        source_count,
        message_count: report.unique_records,
        raw_bytes: report.raw_bytes,
        cross_source_overlap_identities: report.cross_source_overlap_identities,
        cutover_cursors,
    })
}

pub fn prepare_history_message_request(
    data_space_id: &DataSpaceId,
    source: &HistorySourceManifest,
    owner_id: &str,
    entry: &HistoryManifestEntry,
    raw_blob_ref: BlobRef,
) -> HistoryResult<OperationalAppendRequest> {
    let identity = source_record_identity(
        &source.source_instance_id,
        &entry.source_session_id,
        &entry.source_message_id,
    );
    let object_id = history_object_id(&entry.source_session_id, &entry.source_message_id)?;
    let identity_digest = sha256_hex(identity.as_bytes());
    let event_id = OperationalEventId::new(format!("event:history-message:{identity_digest}"));
    let session_ref = history_session_ref(&source.source_instance_id, &entry.source_session_id);
    let source_message_ref = format!("source-message:sha256:{identity_digest}");
    let payload = HistoryTimelineEntry {
        cursor: 0,
        event_id: event_id.as_str().to_owned(),
        session_ref,
        source_message_ref,
        source_kind: source.source_kind,
        source_instance_id: source.source_instance_id.clone(),
        source_session_id: entry.source_session_id.clone(),
        source_message_id: entry.source_message_id.clone(),
        parent_message_id: entry.parent_message_id.clone(),
        published_at: entry.published_at,
        ordinal: entry.ordinal,
        author: entry.author.clone(),
        surface: entry.surface.clone(),
        channel: entry.channel.clone(),
        text: entry.text.clone(),
        record_kind: entry.record_kind.clone(),
        raw_blob_ref: raw_blob_ref.clone(),
        raw_sha256: entry.raw_sha256.clone(),
        raw_bytes: entry.raw_bytes,
        metadata: entry.metadata.clone(),
    };
    let canonical_json = serde_json::to_string(&payload)?;
    let v2_identity_key =
        history_v2_identity_key(&source.source_instance_id, &object_id, &canonical_json)?;
    Ok(OperationalAppendRequest {
        expected_stream_version: 0,
        event: OperationalEvent {
            event_id: event_id.clone(),
            data_space_id: data_space_id.clone(),
            stream_id: format!("history-message:{identity_digest}"),
            stream_version: 1,
            event_type: HISTORY_MESSAGE_EVENT_TYPE.to_owned(),
            occurred_at: entry.published_at,
            actor_type: "history_source".to_owned(),
            actor_id: Some(entry.author.clone()),
            correlation_id: None,
            causation_id: None,
            observation: Observation {
                id: ObservationId::new(format!(
                    "observation:history-message:{}",
                    sha256_hex(format!("{identity}:{}", entry.raw_sha256).as_bytes())
                )),
                schema: SchemaRef::new(HISTORY_SCHEMA),
                schema_version: SemVer::new(HISTORY_SCHEMA_VERSION),
                observer: ObserverRef::new("obs:history-importer"),
                source_system: Some(SourceSystemRef::new(format!(
                    "sys:history:{}",
                    source_kind_name(source.source_kind)
                ))),
                actor: Some(EntityRef::new(format!("history-author:{}", entry.author))),
                authority_model: AuthorityModel::LakeAuthoritative,
                capture_model: CaptureModel::Event,
                subject: EntityRef::new(format!("history-message:{identity_digest}")),
                target: Some(EntityRef::new(format!("owner:{owner_id}"))),
                payload: serde_json::to_value(payload)?,
                attachments: vec![raw_blob_ref],
                published: entry.published_at,
                recorded_at: entry.published_at,
                consent: None,
                idempotency_key: v2_identity_key,
                meta: serde_json::json!({
                    "canonical_json": canonical_json,
                    "source_container": entry.source_session_id,
                    "data_space_id": data_space_id.as_str(),
                    "event_id": event_id.as_str(),
                    HISTORY_SOURCE_INSTANCE_META: source.source_instance_id,
                    HISTORY_OBJECT_ID_META: object_id,
                    "source_message_id": entry.source_message_id,
                    "raw_sha256": entry.raw_sha256,
                }),
            },
        },
    })
}

pub fn prepare_history_receipt_request(
    data_space_id: &DataSpaceId,
    receipt: &HistoryImportReceipt,
) -> HistoryResult<OperationalAppendRequest> {
    let event_id =
        OperationalEventId::new(format!("event:history-import:{}", receipt.manifest_digest));
    let payload = serde_json::to_value(receipt)?;
    let canonical_json = serde_json::to_string(receipt)?;
    Ok(OperationalAppendRequest {
        expected_stream_version: 0,
        event: OperationalEvent {
            event_id: event_id.clone(),
            data_space_id: data_space_id.clone(),
            stream_id: format!("history-import:{}", receipt.manifest_digest),
            stream_version: 1,
            event_type: HISTORY_IMPORT_EVENT_TYPE.to_owned(),
            occurred_at: receipt.captured_at,
            actor_type: "history_importer".to_owned(),
            actor_id: None,
            correlation_id: Some(receipt.inventory_id.clone()),
            causation_id: None,
            observation: Observation {
                id: ObservationId::new(format!(
                    "observation:history-import:{}",
                    receipt.manifest_digest
                )),
                schema: SchemaRef::new(HISTORY_IMPORT_SCHEMA),
                schema_version: SemVer::new(HISTORY_SCHEMA_VERSION),
                observer: ObserverRef::new("obs:history-importer"),
                source_system: Some(SourceSystemRef::new("sys:lethe-history")),
                actor: None,
                authority_model: AuthorityModel::LakeAuthoritative,
                capture_model: CaptureModel::Event,
                subject: EntityRef::new(receipt.receipt_id.clone()),
                target: None,
                payload,
                attachments: vec![],
                published: receipt.captured_at,
                recorded_at: receipt.captured_at,
                consent: None,
                idempotency_key: IdempotencyKey::new(format!(
                    "history-import:{}",
                    receipt.manifest_digest
                )),
                meta: serde_json::json!({
                    "canonical_json": canonical_json,
                    "source_container": receipt.inventory_id,
                    "data_space_id": data_space_id.as_str(),
                    "event_id": event_id.as_str(),
                    "manifest_digest": receipt.manifest_digest,
                }),
            },
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryTimelineEntry {
    #[serde(default)]
    pub cursor: u64,
    pub event_id: String,
    pub session_ref: String,
    pub source_message_ref: String,
    pub source_kind: HistorySourceKind,
    pub source_instance_id: String,
    pub source_session_id: String,
    pub source_message_id: String,
    pub parent_message_id: Option<String>,
    pub published_at: DateTime<Utc>,
    pub ordinal: u64,
    pub author: String,
    pub surface: String,
    pub channel: String,
    pub text: String,
    pub record_kind: HistoryRecordKind,
    pub raw_blob_ref: BlobRef,
    pub raw_sha256: String,
    pub raw_bytes: u64,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistorySessionSummary {
    pub session_ref: String,
    pub source_kind: HistorySourceKind,
    pub source_instance_id: String,
    pub source_session_id: String,
    pub first_message_at: DateTime<Utc>,
    pub last_message_at: DateTime<Utc>,
    pub message_count: u64,
    pub surfaces: Vec<String>,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenCommitment {
    pub commitment_id: String,
    pub text: String,
    pub event_id: String,
    pub published_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateEntry {
    pub state_key: String,
    pub value: String,
    pub event_id: String,
    pub published_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentStateIndexEntry {
    pub state_key: String,
    pub event_id: String,
    pub published_at: DateTime<Utc>,
    pub value_bytes: u64,
    pub value_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryQueryOperation {
    ListSessions,
    ListOpenCommitments,
    GetCurrentState,
    ReadTimeline,
    ReadRaw,
    Search,
    ResolveReference,
}

impl HistoryQueryOperation {
    fn name(self) -> &'static str {
        match self {
            Self::ListSessions => "list_sessions",
            Self::ListOpenCommitments => "list_open_commitments",
            Self::GetCurrentState => "get_current_state",
            Self::ReadTimeline => "read_timeline",
            Self::ReadRaw => "read_raw",
            Self::Search => "search",
            Self::ResolveReference => "resolve_reference",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryQueryRequest {
    pub data_space_id: DataSpaceId,
    pub operation: HistoryQueryOperation,
    pub argument: serde_json::Value,
    pub page_cursor: Option<String>,
    pub max_result_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryQueryResponse {
    pub result_json: serde_json::Value,
    pub next_cursor: Option<String>,
    pub source_cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryRawResult {
    pub message_id: String,
    pub blob_ref: BlobRef,
    pub sha256: String,
    pub bytes: u64,
    pub encoding: String,
    pub content_base64: String,
}

#[derive(Debug, Clone, Default)]
pub struct HistoryProjection {
    entries: Vec<HistoryTimelineEntry>,
    source_watermark: u64,
    query_revision: u64,
}

impl HistoryProjection {
    pub fn rebuild<T: OperationalStoragePorts + ?Sized>(store: &T) -> HistoryResult<Self> {
        let mut projection = Self::default();
        projection.refresh_from(store)?;
        Ok(projection)
    }

    pub fn refresh_from<T: OperationalStoragePorts + ?Sized>(
        &mut self,
        store: &T,
    ) -> HistoryResult<()> {
        let mut after_cursor = self.source_watermark;
        let mut changed = false;
        loop {
            let page = store.operational_event_page(after_cursor, 512)?;
            if page.is_empty() {
                break;
            }
            after_cursor = page.last().map(|stored| stored.cursor).ok_or_else(|| {
                HistoryError::Invariant("history projection received an empty page".to_owned())
            })?;
            for stored in page {
                after_cursor = stored.cursor;
                if stored.event.event_type != HISTORY_MESSAGE_EVENT_TYPE {
                    continue;
                }
                let mut entry: HistoryTimelineEntry =
                    serde_json::from_value(stored.event.observation.payload.clone())?;
                if entry.event_id != stored.event.event_id.as_str() {
                    return Err(HistoryError::Invariant(format!(
                        "history payload event_id mismatch for {}",
                        stored.event.event_id
                    )));
                }
                if stored.event.observation.attachments.as_slice()
                    != std::slice::from_ref(&entry.raw_blob_ref)
                {
                    return Err(HistoryError::Invariant(format!(
                        "history raw attachment mismatch for {}",
                        stored.event.event_id
                    )));
                }
                entry.cursor = stored.cursor;
                self.entries.push(entry);
                self.query_revision = stored.cursor;
                changed = true;
            }
        }
        if changed {
            self.entries.sort_by(timeline_cmp);
        }
        self.source_watermark = after_cursor;
        Ok(())
    }

    pub fn source_watermark(&self) -> u64 {
        self.source_watermark
    }

    pub fn list_sessions(
        &self,
        after_session_ref: Option<&str>,
        limit: usize,
    ) -> HistoryResult<Vec<HistorySessionSummary>> {
        validate_limit(limit)?;
        let mut grouped = BTreeMap::<String, Vec<&HistoryTimelineEntry>>::new();
        for entry in &self.entries {
            grouped
                .entry(entry.session_ref.clone())
                .or_default()
                .push(entry);
        }
        grouped
            .into_iter()
            .filter(|(session_ref, _)| {
                after_session_ref.is_none_or(|after| session_ref.as_str() > after)
            })
            .take(limit)
            .map(|(session_ref, entries)| {
                let first = entries.first().ok_or_else(|| {
                    HistoryError::Invariant(format!("history session {session_ref} is empty"))
                })?;
                let last = entries.last().ok_or_else(|| {
                    HistoryError::Invariant(format!("history session {session_ref} is empty"))
                })?;
                let mut surfaces = entries
                    .iter()
                    .map(|entry| entry.surface.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let mut channels = entries
                    .iter()
                    .map(|entry| entry.channel.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                surfaces.sort();
                channels.sort();
                Ok(HistorySessionSummary {
                    session_ref,
                    source_kind: first.source_kind,
                    source_instance_id: first.source_instance_id.clone(),
                    source_session_id: first.source_session_id.clone(),
                    first_message_at: first.published_at,
                    last_message_at: last.published_at,
                    message_count: u64::try_from(entries.len()).map_err(|_| {
                        HistoryError::Invariant(
                            "history session message count does not fit u64".to_owned(),
                        )
                    })?,
                    surfaces,
                    channels,
                })
            })
            .collect()
    }

    pub fn read_timeline(
        &self,
        session_ref: &str,
        after_cursor: u64,
        limit: usize,
    ) -> HistoryResult<Vec<HistoryTimelineEntry>> {
        validate_non_blank("session_ref", session_ref)?;
        validate_limit(limit)?;
        Ok(self
            .entries
            .iter()
            .filter(|entry| entry.session_ref == session_ref && entry.cursor > after_cursor)
            .take(limit)
            .cloned()
            .collect())
    }

    pub fn read_raw<T: OperationalStoragePorts + ?Sized>(
        &self,
        store: &T,
        reference: &str,
    ) -> HistoryResult<Vec<u8>> {
        let entry = self.resolve_reference(reference)?;
        let raw = store
            .get_blob(&entry.raw_blob_ref)?
            .ok_or_else(|| HistoryError::NotFound(entry.raw_blob_ref.to_string()))?;
        let digest = sha256_hex(&raw);
        if digest != entry.raw_sha256 {
            return Err(HistoryError::Invariant(format!(
                "history raw blob digest mismatch for {reference}"
            )));
        }
        Ok(raw)
    }

    pub fn search(&self, query: &str, limit: usize) -> HistoryResult<Vec<HistoryTimelineEntry>> {
        validate_non_blank("query", query)?;
        validate_limit(limit)?;
        let query = query.to_lowercase();
        Ok(self
            .entries
            .iter()
            .filter(|entry| entry.text.to_lowercase().contains(&query))
            .take(limit)
            .cloned()
            .collect())
    }

    pub fn resolve_reference(&self, reference: &str) -> HistoryResult<HistoryTimelineEntry> {
        validate_non_blank("reference", reference)?;
        self.entries
            .iter()
            .find(|entry| {
                entry.event_id == reference
                    || entry.source_message_ref == reference
                    || entry.raw_blob_ref.as_str() == reference
            })
            .cloned()
            .ok_or_else(|| HistoryError::NotFound(reference.to_owned()))
    }

    pub fn list_open_commitments(&self) -> Vec<OpenCommitment> {
        let mut latest = BTreeMap::<String, (&HistoryTimelineEntry, CommitmentStatus)>::new();
        for entry in &self.entries {
            let HistoryRecordKind::Commitment {
                commitment_id,
                status,
            } = &entry.record_kind
            else {
                continue;
            };
            latest.insert(commitment_id.clone(), (entry, *status));
        }
        latest
            .into_iter()
            .filter_map(|(commitment_id, (entry, status))| {
                (status == CommitmentStatus::Open).then(|| OpenCommitment {
                    commitment_id,
                    text: entry.text.clone(),
                    event_id: entry.event_id.clone(),
                    published_at: entry.published_at,
                })
            })
            .collect()
    }

    pub fn get_current_state(&self) -> Vec<CurrentStateEntry> {
        let mut latest = BTreeMap::<String, &HistoryTimelineEntry>::new();
        for entry in &self.entries {
            let HistoryRecordKind::CurrentState { state_key } = &entry.record_kind else {
                continue;
            };
            latest.insert(state_key.clone(), entry);
        }
        latest
            .into_iter()
            .map(|(state_key, entry)| CurrentStateEntry {
                state_key,
                value: entry.text.clone(),
                event_id: entry.event_id.clone(),
                published_at: entry.published_at,
            })
            .collect()
    }

    pub fn query<T: OperationalStoragePorts + ?Sized>(
        &self,
        store: &T,
        request: &HistoryQueryRequest,
    ) -> HistoryResult<HistoryQueryResponse> {
        if request.data_space_id != *store.data_space_id() {
            return Err(HistoryError::Invariant(format!(
                "history query data space {} does not match Lake {}",
                request.data_space_id,
                store.data_space_id()
            )));
        }
        if request.max_result_bytes == 0 {
            return Err(HistoryError::Invariant(
                "max_result_bytes must be positive".to_owned(),
            ));
        }
        let offset = parse_query_cursor(
            request.page_cursor.as_deref(),
            request.operation,
            self.query_revision,
        )?;
        let (result_json, next_offset) = match request.operation {
            HistoryQueryOperation::ListSessions => {
                let _: EmptyArgument = parse_argument(&request.argument)?;
                let sessions = self.all_sessions()?;
                page_values(&sessions, offset, request.max_result_bytes)?
            }
            HistoryQueryOperation::ListOpenCommitments => {
                let _: EmptyArgument = parse_argument(&request.argument)?;
                let commitments = self.list_open_commitments();
                page_values(&commitments, offset, request.max_result_bytes)?
            }
            HistoryQueryOperation::GetCurrentState => {
                let argument: CurrentStateArgument = parse_argument(&request.argument)?;
                let current_state = self.get_current_state();
                if let Some(state_key) = argument.state_key {
                    require_first_page(offset)?;
                    validate_non_blank("argument.state_key", &state_key)?;
                    let entry = current_state
                        .into_iter()
                        .find(|entry| entry.state_key == state_key)
                        .ok_or(HistoryError::NotFound(state_key))?;
                    let value = serde_json::to_value(entry)?;
                    ensure_value_size(&value, request.max_result_bytes)?;
                    (value, None)
                } else {
                    let index = current_state
                        .into_iter()
                        .map(|entry| CurrentStateIndexEntry {
                            state_key: entry.state_key,
                            event_id: entry.event_id,
                            published_at: entry.published_at,
                            value_bytes: entry.value.len() as u64,
                            value_sha256: sha256_hex(entry.value.as_bytes()),
                        })
                        .collect::<Vec<_>>();
                    page_values(&index, offset, request.max_result_bytes)?
                }
            }
            HistoryQueryOperation::ReadTimeline => {
                let argument: TimelineArgument = parse_argument(&request.argument)?;
                validate_non_blank("argument.session_id", &argument.session_id)?;
                let entries = self
                    .entries
                    .iter()
                    .filter(|entry| entry.session_ref == argument.session_id)
                    .cloned()
                    .collect::<Vec<_>>();
                page_values(&entries, offset, request.max_result_bytes)?
            }
            HistoryQueryOperation::Search => {
                let argument: SearchArgument = parse_argument(&request.argument)?;
                validate_non_blank("argument.query", &argument.query)?;
                let query = argument.query.to_lowercase();
                let mut matches = self
                    .entries
                    .iter()
                    .filter(|entry| entry.text.to_lowercase().contains(&query))
                    .cloned()
                    .collect::<Vec<_>>();
                matches.sort_by_key(|entry| entry.cursor);
                page_values(&matches, offset, request.max_result_bytes)?
            }
            HistoryQueryOperation::ResolveReference => {
                require_first_page(offset)?;
                let argument: ReferenceArgument = parse_argument(&request.argument)?;
                let entry = self.resolve_reference(&argument.reference_id)?;
                let value = serde_json::to_value(entry)?;
                ensure_value_size(&value, request.max_result_bytes)?;
                (value, None)
            }
            HistoryQueryOperation::ReadRaw => {
                require_first_page(offset)?;
                let argument: RawArgument = parse_argument(&request.argument)?;
                let entry = self.resolve_reference(&argument.message_id)?;
                let raw = self.read_raw(store, &argument.message_id)?;
                let value = serde_json::to_value(HistoryRawResult {
                    message_id: entry.event_id,
                    blob_ref: entry.raw_blob_ref,
                    sha256: entry.raw_sha256,
                    bytes: entry.raw_bytes,
                    encoding: "base64".to_owned(),
                    content_base64: BASE64_STANDARD.encode(raw),
                })?;
                ensure_value_size(&value, request.max_result_bytes)?;
                (value, None)
            }
        };
        Ok(HistoryQueryResponse {
            result_json,
            next_cursor: next_offset.map(|next_offset| {
                format_query_cursor(request.operation, self.query_revision, next_offset)
            }),
            source_cursor: format!("operational:{}", self.source_watermark),
        })
    }

    fn all_sessions(&self) -> HistoryResult<Vec<HistorySessionSummary>> {
        let mut sessions = self.list_sessions(None, usize::MAX)?;
        sessions.sort_by(|left, right| {
            (
                left.first_message_at,
                &left.source_instance_id,
                &left.source_session_id,
            )
                .cmp(&(
                    right.first_message_at,
                    &right.source_instance_id,
                    &right.source_session_id,
                ))
        });
        Ok(sessions)
    }
}

pub fn query_history<T: OperationalStoragePorts + ?Sized>(
    store: &T,
    request: &HistoryQueryRequest,
) -> HistoryResult<HistoryQueryResponse> {
    HistoryProjection::rebuild(store)?.query(store, request)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LetheObservationScan {
    pub cutover_append_seq: u64,
    pub examined_observations: u64,
    pub included_records: u64,
}

/// The explicit destination source for a conversation recovered from an
/// existing LETHE Lake.  Coding-agent observations are deliberately absent:
/// their authoritative native archives are imported by their own adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LetheConversationPartition {
    ClaudeAi,
    ResidualLethe,
}

pub fn lethe_observation_cutover_cursor(max_append_seq: u64) -> String {
    format!("lethe-observation:{max_append_seq}")
}

pub fn visit_lethe_conversation_observations<T, F>(
    store: &T,
    max_append_seq: u64,
    page_size: usize,
    claude_ai_source_instance: &str,
    residual_upstream_instances: &BTreeMap<String, String>,
    mut visitor: F,
) -> HistoryResult<LetheObservationScan>
where
    T: ObservationStore + ?Sized,
    F: FnMut(LetheConversationPartition, HistoryRawRecord) -> HistoryResult<()>,
{
    validate_limit(page_size)?;
    validate_non_blank("claude_ai_source_instance", claude_ai_source_instance)?;
    let stats = store.observation_stats()?;
    if max_append_seq > stats.max_append_seq {
        return Err(HistoryError::Invariant(format!(
            "LETHE cutover append_seq {max_append_seq} exceeds current source watermark {}",
            stats.max_append_seq
        )));
    }
    let mut after_append_seq = 0_u64;
    let mut examined_observations = 0_u64;
    let mut included_records = 0_u64;
    while after_append_seq < max_append_seq {
        let page = store.observation_page(after_append_seq, page_size)?;
        if page.is_empty() {
            return Err(HistoryError::Invariant(format!(
                "LETHE observation source ended at append_seq {after_append_seq} before cutover {max_append_seq}"
            )));
        }
        let mut reached_cutover = false;
        for stored in page {
            if stored.append_seq <= after_append_seq {
                return Err(HistoryError::Invariant(format!(
                    "LETHE observation page is not strictly increasing after {after_append_seq}"
                )));
            }
            if stored.append_seq > max_append_seq {
                reached_cutover = true;
                break;
            }
            after_append_seq = stored.append_seq;
            examined_observations = examined_observations.checked_add(1).ok_or_else(|| {
                HistoryError::Invariant("LETHE examined observation count overflow".to_owned())
            })?;
            let source_system = stored
                .observation
                .source_system
                .as_ref()
                .map(|source| source.as_str());
            let partition = match source_system {
                // Both historical Claude desktop identifiers are one explicit
                // Claude AI source.  Do not infer an instance from the Lake.
                Some("sys:claude") | Some("sys:claude-ai") => LetheConversationPartition::ClaudeAi,
                // Native Claude Code and Codex are imported directly.  Keeping
                // them here would produce a cross-source native-identity overlap.
                Some("sys:claude-code") | Some("sys:codex") => {
                    if after_append_seq == max_append_seq {
                        reached_cutover = true;
                    }
                    continue;
                }
                Some(_) => LetheConversationPartition::ResidualLethe,
                None => {
                    if after_append_seq == max_append_seq {
                        reached_cutover = true;
                    }
                    continue;
                }
            };
            let claude_ai_instances;
            let upstream_instances = match partition {
                LetheConversationPartition::ClaudeAi => {
                    claude_ai_instances = BTreeMap::from([
                        (
                            "sys:claude".to_owned(),
                            claude_ai_source_instance.to_owned(),
                        ),
                        (
                            "sys:claude-ai".to_owned(),
                            claude_ai_source_instance.to_owned(),
                        ),
                    ]);
                    &claude_ai_instances
                }
                LetheConversationPartition::ResidualLethe => residual_upstream_instances,
            };
            if let Some(record) = lethe_observation_history_record(&stored, upstream_instances)? {
                visitor(partition, record)?;
                included_records = included_records.checked_add(1).ok_or_else(|| {
                    HistoryError::Invariant("LETHE included history count overflow".to_owned())
                })?;
            }
            if after_append_seq == max_append_seq {
                reached_cutover = true;
                break;
            }
        }
        if reached_cutover {
            break;
        }
    }
    Ok(LetheObservationScan {
        cutover_append_seq: max_append_seq,
        examined_observations,
        included_records,
    })
}

pub fn lethe_observation_history_record(
    stored: &StoredObservation,
    upstream_instances: &BTreeMap<String, String>,
) -> HistoryResult<Option<HistoryRawRecord>> {
    let observation = &stored.observation;
    let Some(source_system) = observation
        .source_system
        .as_ref()
        .map(|source| source.as_str())
    else {
        return Ok(None);
    };
    if source_system == "sys:lethe-history"
        || observation.schema.as_str() == HISTORY_SCHEMA
        || observation.schema.as_str() == HISTORY_IMPORT_SCHEMA
    {
        return Ok(None);
    }
    let payload = &observation.payload;
    let (
        upstream_kind,
        session_id,
        native_message_id,
        parent_message_id,
        author,
        surface,
        channel,
        text,
    ) = match source_system {
        "sys:claude" | "sys:claude-ai" => (
            "claude_ai",
            required_json_string(payload, "conversation_uuid")?,
            required_json_string(payload, "message_uuid")?,
            optional_json_string(payload, "parent_message_uuid"),
            required_json_string(payload, "sender")?,
            "claude_ai".to_owned(),
            "local".to_owned(),
            json_string(payload, "text")?,
        ),
        "sys:chatgpt" => (
            "chatgpt",
            required_json_string(payload, "conversation_id")?,
            required_json_string(payload, "message_id")?,
            optional_json_string(payload, "parent_message_id"),
            required_json_string(payload, "sender")?,
            "chatgpt".to_owned(),
            "local".to_owned(),
            json_string(payload, "text")?,
        ),
        "sys:claude-code" | "sys:codex" => {
            let item = payload.get("item").ok_or_else(|| {
                HistoryError::Invariant(format!(
                    "LETHE {} observation {} has no item",
                    source_system,
                    observation.id.as_str()
                ))
            })?;
            let (author, text) = match required_json_string(item, "kind")?.as_str() {
                "message" => (
                    required_json_string(item, "role")?,
                    json_string(item, "text")?,
                ),
                "tool_call" => (
                    "assistant".to_owned(),
                    serde_json::to_string(&serde_json::json!({
                        "tool_name": required_json_string(item, "tool_name")?,
                        "references": item.get("references").ok_or_else(|| {
                            HistoryError::Invariant(format!(
                                "LETHE coding-agent observation {} has no tool references",
                                observation.id.as_str()
                            ))
                        })?,
                    }))?,
                ),
                kind => {
                    return Err(HistoryError::Invariant(format!(
                        "LETHE coding-agent observation {} has unknown item kind {kind}",
                        observation.id.as_str()
                    )));
                }
            };
            (
                if source_system == "sys:claude-code" {
                    "claude_code"
                } else {
                    "codex"
                },
                required_json_string(payload, "session_id")?,
                required_json_string(payload, "object_id")?,
                optional_json_string(payload, "parent_message_id"),
                author,
                source_system
                    .strip_prefix("sys:")
                    .ok_or_else(|| {
                        HistoryError::Invariant("invalid LETHE source prefix".to_owned())
                    })?
                    .to_owned(),
                "local".to_owned(),
                text,
            )
        }
        "sys:slack" if observation.schema.as_str() == "schema:slack-message" => {
            let channel = required_json_string(payload, "channel_id")?;
            let message = required_json_string(payload, "ts")?;
            (
                "slack",
                format!(
                    "{}:{}",
                    channel,
                    optional_json_string(payload, "thread_ts").unwrap_or_else(|| message.clone())
                ),
                message,
                optional_json_string(payload, "thread_ts"),
                optional_json_string(payload, "user_name")
                    .or_else(|| optional_json_string(payload, "user_id"))
                    .ok_or_else(|| {
                        HistoryError::Invariant(format!(
                            "LETHE Slack observation {} has no author",
                            observation.id.as_str()
                        ))
                    })?,
                "slack".to_owned(),
                channel,
                json_string(payload, "text")?,
            )
        }
        "sys:discord" if observation.schema.as_str() == "schema:discord-message" => {
            let channel = required_json_string(payload, "channel_id")?;
            let message = required_json_string(payload, "message_id")?;
            (
                "discord",
                format!(
                    "{}:{}",
                    channel,
                    optional_json_string(payload, "referenced_message_id")
                        .unwrap_or_else(|| message.clone())
                ),
                message,
                optional_json_string(payload, "referenced_message_id"),
                optional_json_string(payload, "author_name")
                    .or_else(|| optional_json_string(payload, "author_id"))
                    .ok_or_else(|| {
                        HistoryError::Invariant(format!(
                            "LETHE Discord observation {} has no author",
                            observation.id.as_str()
                        ))
                    })?,
                "discord".to_owned(),
                channel,
                json_string(payload, "content")?,
            )
        }
        _ => return Ok(None),
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    let upstream_instance = upstream_instances.get(source_system).ok_or_else(|| {
        HistoryError::Invariant(format!(
            "LETHE history source {source_system} has no explicit upstream source instance mapping"
        ))
    })?;
    let raw = serde_json::to_vec(observation)?;
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "lethe_observation_id".to_owned(),
        observation.id.as_str().to_owned(),
    );
    metadata.insert("native_message_id".to_owned(), native_message_id.clone());
    metadata.insert(
        "upstream_source_system".to_owned(),
        source_system.to_owned(),
    );
    metadata.insert(
        UPSTREAM_SOURCE_KIND_META.to_owned(),
        upstream_kind.to_owned(),
    );
    metadata.insert(
        UPSTREAM_SOURCE_INSTANCE_META.to_owned(),
        upstream_instance.clone(),
    );
    metadata.insert(UPSTREAM_SESSION_META.to_owned(), session_id.clone());
    metadata.insert(UPSTREAM_MESSAGE_META.to_owned(), native_message_id);
    metadata.insert(
        "upstream_schema".to_owned(),
        observation.schema.as_str().to_owned(),
    );
    metadata.insert("lethe_append_seq".to_owned(), stored.append_seq.to_string());
    Ok(Some(HistoryRawRecord {
        source_session_id: format!("{source_system}:{session_id}"),
        source_message_id: observation.id.as_str().to_owned(),
        parent_message_id: parent_message_id
            .map(|parent| format!("{source_system}:{session_id}:{parent}")),
        published_at: observation.published,
        ordinal: stored.append_seq,
        author,
        surface,
        channel,
        text,
        record_kind: HistoryRecordKind::Message,
        raw,
        metadata,
    }))
}

fn required_json_string(value: &serde_json::Value, field: &str) -> HistoryResult<String> {
    optional_json_string(value, field).ok_or_else(|| {
        HistoryError::Invariant(format!(
            "LETHE history source field {field} must be a non-blank string"
        ))
    })
}

fn json_string(value: &serde_json::Value, field: &str) -> HistoryResult<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            HistoryError::Invariant(format!(
                "LETHE history source field {field} must be a string"
            ))
        })
}

fn optional_json_string(value: &serde_json::Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyArgument {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentStateArgument {
    #[serde(default)]
    state_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimelineArgument {
    session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArgument {
    query: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceArgument {
    reference_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArgument {
    message_id: String,
}

fn parse_argument<T: for<'de> Deserialize<'de>>(value: &serde_json::Value) -> HistoryResult<T> {
    serde_json::from_value(value.clone()).map_err(|error| {
        HistoryError::Invariant(format!("invalid history query argument: {error}"))
    })
}

fn require_first_page(offset: usize) -> HistoryResult<()> {
    if offset == 0 {
        Ok(())
    } else {
        Err(HistoryError::InvalidCursor(
            "single-result operation does not accept a continuation cursor".to_owned(),
        ))
    }
}

fn page_values<T: Serialize>(
    records: &[T],
    offset: usize,
    max_result_bytes: usize,
) -> HistoryResult<(serde_json::Value, Option<usize>)> {
    if offset > records.len() {
        return Err(HistoryError::InvalidCursor(format!(
            "offset {offset} exceeds record count {}",
            records.len()
        )));
    }
    let mut page = Vec::<serde_json::Value>::new();
    let mut index = offset;
    while index < records.len() {
        let value = serde_json::to_value(&records[index])?;
        let mut candidate = page.clone();
        candidate.push(value);
        let candidate_value = serde_json::Value::Array(candidate);
        let bytes = canonical_json_size(&candidate_value)?;
        if bytes > max_result_bytes {
            if page.is_empty() {
                return Err(HistoryError::ResultTooLarge {
                    required: bytes,
                    maximum: max_result_bytes,
                });
            }
            break;
        }
        page = match candidate_value {
            serde_json::Value::Array(values) => values,
            _ => unreachable!("candidate is constructed as an array"),
        };
        index += 1;
    }
    let next_offset = (index < records.len()).then_some(index);
    Ok((serde_json::Value::Array(page), next_offset))
}

fn ensure_value_size(value: &serde_json::Value, max_result_bytes: usize) -> HistoryResult<()> {
    let required = canonical_json_size(value)?;
    if required > max_result_bytes {
        Err(HistoryError::ResultTooLarge {
            required,
            maximum: max_result_bytes,
        })
    } else {
        Ok(())
    }
}

fn canonical_json_size(value: &serde_json::Value) -> HistoryResult<usize> {
    Ok(serde_json::to_vec(value)?.len())
}

fn parse_query_cursor(
    cursor: Option<&str>,
    operation: HistoryQueryOperation,
    current_watermark: u64,
) -> HistoryResult<usize> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let rest = cursor
        .strip_prefix("history-cursor:v1:")
        .ok_or_else(|| HistoryError::InvalidCursor("unsupported cursor prefix".to_owned()))?;
    let parts = rest.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(HistoryError::InvalidCursor(
            "cursor must contain operation, source watermark, and offset".to_owned(),
        ));
    }
    if parts[0] != operation.name() {
        return Err(HistoryError::InvalidCursor(format!(
            "cursor operation {} does not match {}",
            parts[0],
            operation.name()
        )));
    }
    let cursor_watermark = parts[1]
        .parse::<u64>()
        .map_err(|_| HistoryError::InvalidCursor("source watermark is not u64".to_owned()))?;
    if cursor_watermark != current_watermark {
        return Err(HistoryError::CursorStale {
            cursor_source: format!("operational:{cursor_watermark}"),
            current_source: format!("operational:{current_watermark}"),
        });
    }
    parts[2]
        .parse::<usize>()
        .map_err(|_| HistoryError::InvalidCursor("offset is not usize".to_owned()))
}

fn format_query_cursor(
    operation: HistoryQueryOperation,
    source_watermark: u64,
    offset: usize,
) -> String {
    format!(
        "history-cursor:v1:{}:{source_watermark}:{offset}",
        operation.name()
    )
}

pub fn coding_agent_source_inventory(
    source_kind: HistorySourceKind,
    source_instance_id: impl Into<String>,
    cutover_cursor: impl Into<String>,
    ownership: OwnershipAssignment,
    records: &[BackboneHistoryRecord],
) -> HistoryResult<HistorySourceInventory> {
    if !matches!(
        source_kind,
        HistorySourceKind::ClaudeCode | HistorySourceKind::Codex
    ) {
        return Err(HistoryError::Invariant(
            "coding agent records require claude_code or codex source kind".to_owned(),
        ));
    }
    let surface = match source_kind {
        HistorySourceKind::ClaudeCode => "claude_code_native",
        HistorySourceKind::Codex => "codex",
        _ => unreachable!(),
    };
    let source_instance_id = source_instance_id.into();
    let records = records
        .iter()
        .map(|history| {
            let (author, text, record_kind) = match &history.record.item {
                BackboneItem::Message { role, text } => {
                    (role.clone(), text.clone(), HistoryRecordKind::Message)
                }
                BackboneItem::ToolCall {
                    tool_name,
                    references,
                } => (
                    "assistant".to_owned(),
                    serde_json::to_string(&serde_json::json!({
                        "tool_name": tool_name,
                        "references": references,
                    }))?,
                    HistoryRecordKind::Message,
                ),
            };
            let mut metadata = BTreeMap::new();
            metadata.insert(
                "transcript_id".to_owned(),
                history.record.transcript_id.clone(),
            );
            metadata.insert(
                "thread_source".to_owned(),
                history.record.thread_source.clone(),
            );
            metadata.insert(
                "is_sidechain".to_owned(),
                history.record.is_sidechain.to_string(),
            );
            if let Some(parent_thread_id) = &history.record.parent_thread_id {
                metadata.insert("parent_thread_id".to_owned(), parent_thread_id.clone());
            }
            metadata.insert("source_path".to_owned(), history.source_path.clone());
            metadata.insert(
                "native_message_id".to_owned(),
                history.record.object_id.clone(),
            );
            metadata.insert(
                UPSTREAM_SOURCE_KIND_META.to_owned(),
                source_kind_name(source_kind).to_owned(),
            );
            metadata.insert(
                UPSTREAM_SOURCE_INSTANCE_META.to_owned(),
                source_instance_id.to_owned(),
            );
            metadata.insert(
                UPSTREAM_SESSION_META.to_owned(),
                history.record.session_id.clone(),
            );
            metadata.insert(
                UPSTREAM_MESSAGE_META.to_owned(),
                history.record.object_id.clone(),
            );
            let occurrence_suffix =
                sha256_hex(format!("{}\0{}", history.source_path, history.line_number).as_bytes());
            Ok(HistoryRawRecord {
                source_session_id: history.record.session_id.clone(),
                source_message_id: format!("{}@{}", history.record.object_id, occurrence_suffix),
                parent_message_id: history.record.parent_message_id.clone(),
                published_at: history.record.published,
                ordinal: u64::try_from(history.line_number).map_err(|_| {
                    HistoryError::Invariant("coding agent line number does not fit u64".to_owned())
                })?,
                author,
                surface: surface.to_owned(),
                channel: "local".to_owned(),
                text,
                record_kind,
                raw: history.raw.clone(),
                metadata,
            })
        })
        .collect::<HistoryResult<Vec<_>>>()?;
    Ok(HistorySourceInventory {
        source_kind,
        source_instance_id,
        cutover_cursor: cutover_cursor.into(),
        ownership,
        records,
    })
}

fn raw_record_map(request: &HistoryInventoryRequest) -> HistoryResult<BTreeMap<String, Vec<u8>>> {
    let mut result = BTreeMap::new();
    for source in &request.sources {
        for record in &source.records {
            let identity = source_record_identity(
                &source.source_instance_id,
                &record.source_session_id,
                &record.source_message_id,
            );
            match result.get(&identity) {
                Some(raw) if raw != &record.raw => {
                    return Err(HistoryError::SourceIdentityCollision(identity));
                }
                Some(_) => {}
                None => {
                    result.insert(identity, record.raw.clone());
                }
            }
        }
    }
    Ok(result)
}

fn validate_source(source: &HistorySourceInventory) -> HistoryResult<()> {
    validate_non_blank("source_instance_id", &source.source_instance_id)?;
    validate_non_blank("cutover_cursor", &source.cutover_cursor)?;
    match &source.ownership {
        OwnershipAssignment::Personal { owner_id } => validate_non_blank("owner_id", owner_id),
        OwnershipAssignment::Unresolved { reason } => {
            validate_non_blank("ownership unresolved reason", reason)
        }
    }
}

fn validate_record(record: &HistoryRawRecord) -> HistoryResult<()> {
    validate_non_blank("source_session_id", &record.source_session_id)?;
    validate_non_blank("source_message_id", &record.source_message_id)?;
    validate_non_blank("author", &record.author)?;
    validate_non_blank("surface", &record.surface)?;
    validate_non_blank("channel", &record.channel)?;
    if record.raw.is_empty() {
        return Err(HistoryError::Invariant(
            "history raw record must not be empty".to_owned(),
        ));
    }
    history_upstream_identity(record)?;
    match &record.record_kind {
        HistoryRecordKind::Message => {}
        HistoryRecordKind::Decision { decision_id, .. } => {
            validate_non_blank("decision_id", decision_id)?
        }
        HistoryRecordKind::Commitment { commitment_id, .. } => {
            validate_non_blank("commitment_id", commitment_id)?
        }
        HistoryRecordKind::WorkItem {
            work_item_id,
            state,
        } => {
            validate_non_blank("work_item_id", work_item_id)?;
            validate_non_blank("work item state", state)?;
        }
        HistoryRecordKind::Preference { preference_key } => {
            validate_non_blank("preference_key", preference_key)?
        }
        HistoryRecordKind::CurrentState { state_key } => {
            validate_non_blank("state_key", state_key)?
        }
        HistoryRecordKind::NodeMemory { memory_id, node_id } => {
            validate_non_blank("memory_id", memory_id)?;
            validate_non_blank("node_id", node_id)?;
        }
    }
    Ok(())
}

pub fn history_upstream_identity(record: &HistoryRawRecord) -> HistoryResult<Option<String>> {
    let fields = [
        (
            UPSTREAM_SOURCE_KIND_META,
            record.metadata.get(UPSTREAM_SOURCE_KIND_META),
        ),
        (
            UPSTREAM_SOURCE_INSTANCE_META,
            record.metadata.get(UPSTREAM_SOURCE_INSTANCE_META),
        ),
        (
            UPSTREAM_SESSION_META,
            record.metadata.get(UPSTREAM_SESSION_META),
        ),
        (
            UPSTREAM_MESSAGE_META,
            record.metadata.get(UPSTREAM_MESSAGE_META),
        ),
    ];
    if fields.iter().all(|(_, value)| value.is_none()) {
        return Ok(None);
    }
    let mut values = Vec::with_capacity(fields.len());
    for (field, value) in fields {
        let value = value.ok_or_else(|| {
            HistoryError::Invariant(format!(
                "history upstream provenance is incomplete: missing {field}"
            ))
        })?;
        validate_non_blank(field, value)?;
        values.push(value.as_str());
    }
    Ok(Some(values.join("\0")))
}

fn validate_non_blank(field: &str, value: &str) -> HistoryResult<()> {
    if value.trim().is_empty() {
        Err(HistoryError::Invariant(format!(
            "{field} must not be blank"
        )))
    } else {
        Ok(())
    }
}

fn validate_sha256(field: &str, value: &str) -> HistoryResult<()> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(HistoryError::Invariant(format!(
            "{field} must be a SHA-256 hex digest"
        )))
    }
}

fn validate_projection_ref(field: &str, value: &str) -> HistoryResult<()> {
    let digest = value.rsplit(':').next().unwrap_or_default();
    if value.starts_with("history-projection:") {
        validate_sha256(field, digest)
    } else {
        Err(HistoryError::Invariant(format!(
            "{field} must be a history-projection reference"
        )))
    }
}

fn validate_limit(limit: usize) -> HistoryResult<()> {
    if limit == 0 {
        Err(HistoryError::Invariant(
            "history read limit must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn reject_duplicate_required_sources(sources: &[RequiredHistorySource]) -> HistoryResult<()> {
    let mut seen = BTreeSet::new();
    for source in sources {
        validate_non_blank("required source_instance_id", &source.source_instance_id)?;
        let key = source_key(source.source_kind, &source.source_instance_id);
        if !seen.insert(key.clone()) {
            return Err(HistoryError::Invariant(format!(
                "duplicate required history source {key}"
            )));
        }
    }
    Ok(())
}

fn source_key_cmp(
    left: &RequiredHistorySource,
    right: &RequiredHistorySource,
) -> std::cmp::Ordering {
    source_key(left.source_kind, &left.source_instance_id)
        .cmp(&source_key(right.source_kind, &right.source_instance_id))
}

fn manifest_entry_cmp(
    left: &HistoryManifestEntry,
    right: &HistoryManifestEntry,
) -> std::cmp::Ordering {
    (
        left.published_at,
        left.ordinal,
        &left.source_session_id,
        &left.source_message_id,
    )
        .cmp(&(
            right.published_at,
            right.ordinal,
            &right.source_session_id,
            &right.source_message_id,
        ))
}

fn manifest_entry_key(entry: &HistoryManifestEntry) -> String {
    format!(
        "{}\0{:020}\0{}\0{}",
        entry.published_at.to_rfc3339(),
        entry.ordinal,
        entry.source_session_id,
        entry.source_message_id
    )
}

fn update_framed_json<T: Serialize>(hasher: &mut Sha256, value: &T) -> HistoryResult<()> {
    let bytes = serde_json::to_vec(value)?;
    let length = u64::try_from(bytes.len()).map_err(|_| {
        HistoryError::Invariant("manifest frame length does not fit u64".to_owned())
    })?;
    hasher.update(length.to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

fn timeline_cmp(left: &HistoryTimelineEntry, right: &HistoryTimelineEntry) -> std::cmp::Ordering {
    (left.published_at, left.ordinal, left.cursor, &left.event_id).cmp(&(
        right.published_at,
        right.ordinal,
        right.cursor,
        &right.event_id,
    ))
}

fn source_key(source_kind: HistorySourceKind, source_instance_id: &str) -> String {
    format!("{}:{source_instance_id}", source_kind_name(source_kind))
}

fn source_kind_name(source_kind: HistorySourceKind) -> &'static str {
    match source_kind {
        HistorySourceKind::ClaudeCode => "claude_code",
        HistorySourceKind::ClaudeAi => "claude_ai",
        HistorySourceKind::Codex => "codex",
        HistorySourceKind::Intercom => "intercom",
        HistorySourceKind::Lethe => "lethe",
        HistorySourceKind::NaniholdLegacy => "nanihold_legacy",
        HistorySourceKind::SystemSnapshot => "system_snapshot",
    }
}

fn source_record_identity(
    source_instance_id: &str,
    source_session_id: &str,
    source_message_id: &str,
) -> String {
    format!("{source_instance_id}\0{source_session_id}\0{source_message_id}")
}

fn history_session_ref(source_instance_id: &str, source_session_id: &str) -> String {
    format!(
        "history-session:sha256:{}",
        sha256_hex(format!("{source_instance_id}\0{source_session_id}").as_bytes())
    )
}

pub fn history_session_reference(
    source_instance_id: &str,
    source_session_id: &str,
) -> HistoryResult<String> {
    validate_non_blank("source_instance_id", source_instance_id)?;
    validate_non_blank("source_session_id", source_session_id)?;
    Ok(history_session_ref(source_instance_id, source_session_id))
}

pub fn history_object_id(
    source_session_id: &str,
    source_message_id: &str,
) -> HistoryResult<String> {
    validate_non_blank("source_session_id", source_session_id)?;
    validate_non_blank("source_message_id", source_message_id)?;
    Ok(format!("{source_session_id}:{source_message_id}"))
}

pub fn history_v2_identity_key(
    source_instance_id: &str,
    object_id: &str,
    canonical_json: &str,
) -> HistoryResult<IdempotencyKey> {
    validate_non_blank("source_instance_id", source_instance_id)?;
    validate_non_blank("object_id", object_id)?;
    validate_non_blank("canonical_json", canonical_json)?;
    serde_json::from_str::<serde_json::Value>(canonical_json)?;
    Ok(IdempotencyKey::new(format!(
        "{source_instance_id}:{object_id}:{}",
        sha256_hex(canonical_json.as_bytes())
    )))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub mod conformance {
    use super::*;
    use chrono::TimeZone;

    pub fn history_ingestion_round_trip<T: OperationalStoragePorts + ?Sized>(store: &T) {
        let captured_at = Utc.with_ymd_and_hms(2026, 7, 20, 12, 0, 0).unwrap();
        let first_at = Utc.with_ymd_and_hms(2026, 7, 20, 10, 0, 0).unwrap();
        let second_at = Utc.with_ymd_and_hms(2026, 7, 20, 10, 1, 0).unwrap();
        let state_at = Utc.with_ymd_and_hms(2026, 7, 20, 10, 2, 0).unwrap();
        let source = HistorySourceInventory {
            source_kind: HistorySourceKind::Codex,
            source_instance_id: "codex-personal".to_owned(),
            cutover_cursor: "cursor:codex:3".to_owned(),
            ownership: OwnershipAssignment::Personal {
                owner_id: "owner".to_owned(),
            },
            records: vec![
                sample_record("session-1", "message-1", first_at, 1, "はい", b"raw-one"),
                sample_record("session-1", "message-2", second_at, 2, "はい", b"raw-two"),
                HistoryRawRecord {
                    source_session_id: "session-1".to_owned(),
                    source_message_id: "message-3".to_owned(),
                    parent_message_id: Some("message-2".to_owned()),
                    published_at: state_at,
                    ordinal: 3,
                    author: "assistant".to_owned(),
                    surface: "codex".to_owned(),
                    channel: "local".to_owned(),
                    text: "履歴を取り込む".to_owned(),
                    record_kind: HistoryRecordKind::Commitment {
                        commitment_id: "commitment:history".to_owned(),
                        status: CommitmentStatus::Open,
                    },
                    raw: b"raw-three".to_vec(),
                    metadata: BTreeMap::new(),
                },
                HistoryRawRecord {
                    source_session_id: "session-1".to_owned(),
                    source_message_id: "message-4".to_owned(),
                    parent_message_id: Some("message-3".to_owned()),
                    published_at: state_at,
                    ordinal: 4,
                    author: "system".to_owned(),
                    surface: "codex".to_owned(),
                    channel: "local".to_owned(),
                    text: "quota=17%".to_owned(),
                    record_kind: HistoryRecordKind::CurrentState {
                        state_key: "claude_quota".to_owned(),
                    },
                    raw: b"raw-four".to_vec(),
                    metadata: BTreeMap::new(),
                },
                HistoryRawRecord {
                    source_session_id: "session-1".to_owned(),
                    source_message_id: "message-5".to_owned(),
                    parent_message_id: Some("message-4".to_owned()),
                    published_at: state_at,
                    ordinal: 5,
                    author: "system".to_owned(),
                    surface: "nanihold".to_owned(),
                    channel: "local".to_owned(),
                    text: "owner prefers concise status updates".to_owned(),
                    record_kind: HistoryRecordKind::NodeMemory {
                        memory_id: "memory:owner-status".to_owned(),
                        node_id: "node:interface-owner".to_owned(),
                    },
                    raw: b"raw-five".to_vec(),
                    metadata: BTreeMap::new(),
                },
            ],
        };
        let request = HistoryInventoryRequest {
            inventory_id: "inventory:conformance".to_owned(),
            data_space_id: store.data_space_id().clone(),
            captured_at,
            required_sources: vec![RequiredHistorySource {
                source_kind: HistorySourceKind::Codex,
                source_instance_id: "codex-personal".to_owned(),
            }],
            sources: vec![source],
        };
        let report = inventory_history(&request).unwrap();
        assert!(report.ready_for_import);
        assert_eq!(report.unique_records, 5);
        let command = HistoryImportCommand {
            inventory: request.clone(),
            expected_manifest_digest: report.manifest.manifest_digest.clone(),
            admission_generations: BTreeMap::new(),
        };
        let first = import_history(store, &command, 1024).unwrap();
        assert_eq!(first.appended_messages, 5);
        assert_eq!(first.duplicate_messages, 0);
        assert!(!first.receipt_was_duplicate);
        let second = import_history(store, &command, 1024).unwrap();
        assert_eq!(second.appended_messages, 0);
        assert_eq!(second.duplicate_messages, 5);
        assert!(second.receipt_was_duplicate);
        assert_eq!(first.receipt, second.receipt);

        let projection = HistoryProjection::rebuild(store).unwrap();
        let sessions = projection.list_sessions(None, 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].message_count, 5);
        let timeline = projection
            .read_timeline(&sessions[0].session_ref, 0, 10)
            .unwrap();
        assert_eq!(timeline.len(), 5);
        assert_eq!(timeline[0].text, "はい");
        assert_eq!(timeline[1].text, "はい");
        assert_ne!(
            timeline[0].source_message_ref,
            timeline[1].source_message_ref
        );
        assert_eq!(
            projection.read_raw(store, &timeline[0].event_id).unwrap(),
            b"raw-one"
        );
        assert_eq!(projection.search("はい", 10).unwrap().len(), 2);
        assert_eq!(
            projection
                .resolve_reference(&timeline[1].source_message_ref)
                .unwrap()
                .source_message_id,
            "message-2"
        );
        assert_eq!(projection.list_open_commitments().len(), 1);
        assert_eq!(projection.get_current_state()[0].state_key, "claude_quota");

        let commitments = projection.list_open_commitments();
        let commitment_bytes = serde_json::to_vec(&serde_json::json!(commitments))
            .unwrap()
            .len();
        let commitments_response = projection
            .query(
                store,
                &HistoryQueryRequest {
                    data_space_id: store.data_space_id().clone(),
                    operation: HistoryQueryOperation::ListOpenCommitments,
                    argument: serde_json::json!({}),
                    page_cursor: None,
                    max_result_bytes: commitment_bytes,
                },
            )
            .unwrap();
        assert_eq!(
            commitments_response.result_json,
            serde_json::json!(commitments)
        );
        assert_eq!(commitments_response.next_cursor, None);
        assert_eq!(
            commitments_response.source_cursor,
            format!(
                "operational:{}",
                store.operational_event_stats().unwrap().max_cursor
            )
        );
        assert!(matches!(
            projection.query(
                store,
                &HistoryQueryRequest {
                    data_space_id: store.data_space_id().clone(),
                    operation: HistoryQueryOperation::ListOpenCommitments,
                    argument: serde_json::json!({}),
                    page_cursor: Some(format_query_cursor(
                        HistoryQueryOperation::GetCurrentState,
                        projection.source_watermark,
                        0,
                    )),
                    max_result_bytes: commitment_bytes,
                },
            ),
            Err(HistoryError::InvalidCursor(_))
        ));
        assert!(matches!(
            projection.query(
                store,
                &HistoryQueryRequest {
                    data_space_id: store.data_space_id().clone(),
                    operation: HistoryQueryOperation::ListOpenCommitments,
                    argument: serde_json::json!({}),
                    page_cursor: None,
                    max_result_bytes: 1,
                },
            ),
            Err(HistoryError::ResultTooLarge { .. })
        ));

        let current_state = projection.get_current_state();
        let current_state_index = current_state
            .iter()
            .map(|entry| CurrentStateIndexEntry {
                state_key: entry.state_key.clone(),
                event_id: entry.event_id.clone(),
                published_at: entry.published_at,
                value_bytes: entry.value.len() as u64,
                value_sha256: sha256_hex(entry.value.as_bytes()),
            })
            .collect::<Vec<_>>();
        let state_bytes = serde_json::to_vec(&serde_json::json!(current_state_index))
            .unwrap()
            .len();
        let current_state_response = projection
            .query(
                store,
                &HistoryQueryRequest {
                    data_space_id: store.data_space_id().clone(),
                    operation: HistoryQueryOperation::GetCurrentState,
                    argument: serde_json::json!({}),
                    page_cursor: None,
                    max_result_bytes: state_bytes,
                },
            )
            .unwrap();
        assert_eq!(
            current_state_response.result_json,
            serde_json::json!(current_state_index)
        );
        assert_eq!(current_state_response.next_cursor, None);
        assert_eq!(
            current_state_response.source_cursor,
            commitments_response.source_cursor
        );
        let targeted = projection
            .query(
                store,
                &HistoryQueryRequest {
                    data_space_id: store.data_space_id().clone(),
                    operation: HistoryQueryOperation::GetCurrentState,
                    argument: serde_json::json!({"state_key": current_state[0].state_key}),
                    page_cursor: None,
                    max_result_bytes: 65_536,
                },
            )
            .unwrap();
        assert_eq!(targeted.result_json, serde_json::json!(current_state[0]));

        let one_timeline_record_bytes = serde_json::to_vec(&serde_json::json!([timeline[0]]))
            .unwrap()
            .len();
        let first_page_request = HistoryQueryRequest {
            data_space_id: store.data_space_id().clone(),
            operation: HistoryQueryOperation::ReadTimeline,
            argument: serde_json::json!({"session_id": sessions[0].session_ref}),
            page_cursor: None,
            max_result_bytes: one_timeline_record_bytes,
        };
        let first_page = projection.query(store, &first_page_request).unwrap();
        assert_eq!(first_page.result_json.as_array().unwrap().len(), 1);
        assert!(first_page.next_cursor.is_some());
        assert_eq!(
            first_page.source_cursor,
            format!(
                "operational:{}",
                store.operational_event_stats().unwrap().max_cursor
            )
        );
        let second_page = projection
            .query(
                store,
                &HistoryQueryRequest {
                    page_cursor: first_page.next_cursor.clone(),
                    ..first_page_request.clone()
                },
            )
            .unwrap();
        assert!(!second_page.result_json.as_array().unwrap().is_empty());
        assert!(matches!(
            projection.query(
                store,
                &HistoryQueryRequest {
                    data_space_id: store.data_space_id().clone(),
                    operation: HistoryQueryOperation::ReadRaw,
                    argument: serde_json::json!({"message_id": timeline[0].event_id}),
                    page_cursor: None,
                    max_result_bytes: 1,
                },
            ),
            Err(HistoryError::ResultTooLarge { .. })
        ));

        let delta_request = HistoryInventoryRequest {
            inventory_id: "inventory:conformance-delta".to_owned(),
            data_space_id: store.data_space_id().clone(),
            captured_at: Utc.with_ymd_and_hms(2026, 7, 20, 13, 0, 0).unwrap(),
            required_sources: vec![RequiredHistorySource {
                source_kind: HistorySourceKind::Codex,
                source_instance_id: "codex-personal".to_owned(),
            }],
            sources: vec![HistorySourceInventory {
                source_kind: HistorySourceKind::Codex,
                source_instance_id: "codex-personal".to_owned(),
                cutover_cursor: "cursor:codex:4".to_owned(),
                ownership: OwnershipAssignment::Personal {
                    owner_id: "owner".to_owned(),
                },
                records: vec![sample_record(
                    "session-1",
                    "message-6",
                    Utc.with_ymd_and_hms(2026, 7, 20, 10, 3, 0).unwrap(),
                    6,
                    "追加",
                    b"raw-six",
                )],
            }],
        };
        let delta_report = inventory_history(&delta_request).unwrap();
        import_history(
            store,
            &HistoryImportCommand {
                inventory: delta_request,
                expected_manifest_digest: delta_report.manifest.manifest_digest,
                admission_generations: BTreeMap::new(),
            },
            1024,
        )
        .unwrap();
        let current_projection = HistoryProjection::rebuild(store).unwrap();
        assert!(matches!(
            current_projection.query(
                store,
                &HistoryQueryRequest {
                    page_cursor: first_page.next_cursor,
                    ..first_page_request
                },
            ),
            Err(HistoryError::CursorStale { .. })
        ));

        let mut changed = request.clone();
        changed.sources[0].records[0].raw = b"changed".to_vec();
        let mismatch = import_history(
            store,
            &HistoryImportCommand {
                inventory: changed,
                expected_manifest_digest: report.manifest.manifest_digest.clone(),
                admission_generations: BTreeMap::new(),
            },
            1024,
        );
        assert!(matches!(
            mismatch,
            Err(HistoryError::ManifestMismatch { .. })
                | Err(HistoryError::SourceIdentityCollision(_))
        ));

        let mut unresolved = request;
        unresolved.inventory_id = "inventory:unresolved".to_owned();
        unresolved.sources[0].ownership = OwnershipAssignment::Unresolved {
            reason: "owner must choose personal or company".to_owned(),
        };
        let unresolved_report = inventory_history(&unresolved).unwrap();
        assert!(!unresolved_report.ready_for_import);
        assert!(matches!(
            import_history(
                store,
                &HistoryImportCommand {
                    inventory: unresolved,
                    expected_manifest_digest: unresolved_report.manifest.manifest_digest,
                    admission_generations: BTreeMap::new(),
                },
                1024,
            ),
            Err(HistoryError::UnresolvedOwnership(_))
        ));
    }

    pub(super) fn sample_record(
        session: &str,
        message: &str,
        published_at: DateTime<Utc>,
        ordinal: u64,
        text: &str,
        raw: &[u8],
    ) -> HistoryRawRecord {
        HistoryRawRecord {
            source_session_id: session.to_owned(),
            source_message_id: message.to_owned(),
            parent_message_id: None,
            published_at,
            ordinal,
            author: "owner".to_owned(),
            surface: "codex".to_owned(),
            channel: "local".to_owned(),
            text: text.to_owned(),
            record_kind: HistoryRecordKind::Message,
            raw: raw.to_vec(),
            metadata: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use lethe_storage_api::OperationalEventStore;
    use lethe_storage_postgres::PostgresOperationalEventStore;
    use lethe_storage_sqlite::{SqliteOperationalEventStore, SqlitePersistence};

    #[test]
    fn history_ingestion_conforms_on_sqlite_operational_store() {
        let tmp = std::env::temp_dir().join(format!("lethe-history-test-{}", uuid::Uuid::now_v7()));
        let store = SqliteOperationalEventStore::open(
            DataSpaceId::new("space:personal"),
            &tmp.join("personal.sqlite3"),
            &tmp.join("blobs"),
            &[7; 32],
        )
        .unwrap();
        conformance::history_ingestion_round_trip(&store);
        let _ = std::fs::remove_dir_all(tmp);
    }

    #[test]
    fn history_projection_refresh_advances_over_non_history_events_without_duplicate_entries() {
        let tmp =
            std::env::temp_dir().join(format!("lethe-history-refresh-{}", uuid::Uuid::now_v7()));
        let store = SqliteOperationalEventStore::open(
            DataSpaceId::new("space:personal"),
            &tmp.join("personal.sqlite3"),
            &tmp.join("blobs"),
            &[7; 32],
        )
        .unwrap();
        conformance::history_ingestion_round_trip(&store);
        let mut projection = HistoryProjection::rebuild(&store).unwrap();
        let initial_watermark = projection.source_watermark();
        let continuation = format_query_cursor(
            HistoryQueryOperation::ListSessions,
            projection.query_revision,
            1,
        );
        let initial_sessions = projection.list_sessions(None, 100).unwrap();

        let receipt = HistoryImportReceipt {
            receipt_id: "history-receipt:refresh".to_owned(),
            inventory_id: "inventory:refresh".to_owned(),
            data_space_id: store.data_space_id().clone(),
            manifest_digest: "f".repeat(64),
            captured_at: Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
            source_count: 1,
            message_count: 0,
            raw_bytes: 0,
            cross_source_overlap_identities: 0,
            cutover_cursors: BTreeMap::new(),
        };
        store
            .append_operational_event(
                &prepare_history_receipt_request(store.data_space_id(), &receipt).unwrap(),
            )
            .unwrap();

        projection.refresh_from(&store).unwrap();
        assert_eq!(projection.source_watermark(), initial_watermark + 1);
        assert_eq!(
            projection.list_sessions(None, 100).unwrap(),
            initial_sessions
        );
        assert!(
            projection
                .query(
                    &store,
                    &HistoryQueryRequest {
                        data_space_id: store.data_space_id().clone(),
                        operation: HistoryQueryOperation::ListSessions,
                        argument: serde_json::json!({}),
                        page_cursor: Some(continuation),
                        max_result_bytes: 65_536,
                    },
                )
                .is_ok()
        );
        let _ = std::fs::remove_dir_all(tmp);
    }

    #[test]
    #[ignore = "requires a clean LETHE_TEST_POSTGRES_HISTORY_SCHEMA and role provisioning"]
    fn history_ingestion_conforms_on_postgres_operational_store() {
        let dsn = std::env::var("LETHE_TEST_POSTGRES_DSN").unwrap();
        let schema = std::env::var("LETHE_TEST_POSTGRES_HISTORY_SCHEMA").unwrap();
        let role = std::env::var("LETHE_TEST_POSTGRES_ROLE").unwrap();
        let store = PostgresOperationalEventStore::connect_no_tls(
            DataSpaceId::new("space:personal"),
            &dsn,
            &schema,
            &role,
        )
        .unwrap();
        conformance::history_ingestion_round_trip(&store);
    }

    #[test]
    fn existing_lethe_conversations_are_paged_and_history_events_are_not_reingested() {
        let tmp =
            std::env::temp_dir().join(format!("lethe-history-source-{}", uuid::Uuid::now_v7()));
        let store =
            SqlitePersistence::open(&tmp.join("personal.sqlite3"), &tmp.join("blobs"), &[7; 32])
                .unwrap();
        for (source, schema, payload) in [
            (
                "sys:claude-ai",
                "schema:claude-message",
                serde_json::json!({
                    "conversation_uuid": "conversation-1",
                    "message_uuid": "message-1",
                    "parent_message_uuid": null,
                    "sender": "human",
                    "text": "同じ本文"
                }),
            ),
            (
                "sys:chatgpt",
                "schema:chatgpt-message",
                serde_json::json!({
                    "conversation_id": "conversation-2",
                    "message_id": "message-2",
                    "parent_message_id": null,
                    "sender": "user",
                    "text": "同じ本文"
                }),
            ),
            (
                "sys:claude-code",
                "schema:coding-agent-history",
                serde_json::json!({
                    "session_id": "claude-code-session",
                    "object_id": "claude-code-message",
                    "parent_message_id": null,
                    "item": {"kind": "message", "role": "user", "text": "native archive wins"}
                }),
            ),
            (
                "sys:codex",
                "schema:coding-agent-history",
                serde_json::json!({
                    "session_id": "codex-session",
                    "object_id": "codex-message",
                    "parent_message_id": null,
                    "item": {"kind": "message", "role": "user", "text": "native archive wins"}
                }),
            ),
            (
                "sys:lethe-history",
                HISTORY_SCHEMA,
                serde_json::json!({"text": "must not loop"}),
            ),
        ] {
            store
                .append_observation(&sample_lethe_observation(source, schema, payload))
                .unwrap();
        }
        let cutover = store.observation_stats().unwrap().max_append_seq;
        let residual_upstream_instances =
            BTreeMap::from([("sys:chatgpt".to_owned(), "chatgpt-personal".to_owned())]);
        let mut records = Vec::new();
        let scan = visit_lethe_conversation_observations(
            &store,
            cutover,
            1,
            "claude-ai-personal",
            &residual_upstream_instances,
            |partition, record| {
                records.push((partition, record));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(scan.examined_observations, 5);
        assert_eq!(scan.included_records, 2);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1.text, records[1].1.text);
        assert_ne!(
            records[0].1.source_message_id,
            records[1].1.source_message_id
        );
        assert_eq!(records[0].0, LetheConversationPartition::ClaudeAi);
        assert_eq!(records[1].0, LetheConversationPartition::ResidualLethe);
        assert_eq!(
            records[0].1.metadata["upstream_source_system"],
            "sys:claude-ai"
        );
        assert_eq!(
            records[0].1.metadata[UPSTREAM_SOURCE_INSTANCE_META],
            "claude-ai-personal"
        );
        assert_eq!(
            lethe_observation_cutover_cursor(cutover),
            format!("lethe-observation:{cutover}")
        );
        let _ = std::fs::remove_dir_all(tmp);
    }

    fn sample_lethe_observation(
        source: &str,
        schema: &str,
        payload: serde_json::Value,
    ) -> Observation {
        let published = Utc.with_ymd_and_hms(2026, 7, 20, 12, 0, 0).unwrap();
        let id = Observation::new_id();
        Observation {
            id: id.clone(),
            schema: SchemaRef::new(schema),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: Some(SourceSystemRef::new(source)),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:test:{}", id.as_str())),
            target: None,
            payload,
            attachments: vec![],
            published,
            recorded_at: published,
            consent: None,
            idempotency_key: IdempotencyKey::new(format!("test:{}", id.as_str())),
            meta: serde_json::json!({
                "canonical_json": format!("canonical:{}", id.as_str()),
                "source_container": "history-source-test"
            }),
        }
    }

    #[test]
    fn same_text_with_distinct_native_ids_is_not_deduplicated() {
        let request = HistoryInventoryRequest {
            inventory_id: "inventory:test".to_owned(),
            data_space_id: DataSpaceId::new("space:personal"),
            captured_at: Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 0).unwrap(),
            required_sources: vec![RequiredHistorySource {
                source_kind: HistorySourceKind::Intercom,
                source_instance_id: "intercom-personal".to_owned(),
            }],
            sources: vec![HistorySourceInventory {
                source_kind: HistorySourceKind::Intercom,
                source_instance_id: "intercom-personal".to_owned(),
                cutover_cursor: "outbox:2".to_owned(),
                ownership: OwnershipAssignment::Personal {
                    owner_id: "owner".to_owned(),
                },
                records: vec![
                    conformance::sample_record(
                        "discord",
                        "1",
                        Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 1).unwrap(),
                        1,
                        "はい",
                        b"{\"id\":\"1\",\"text\":\"yes\"}",
                    ),
                    conformance::sample_record(
                        "discord",
                        "2",
                        Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 2).unwrap(),
                        2,
                        "はい",
                        b"{\"id\":\"2\",\"text\":\"yes\"}",
                    ),
                ],
            }],
        };
        assert_eq!(inventory_history(&request).unwrap().unique_records, 2);
    }

    #[test]
    fn history_message_identity_uses_the_v2_formula_and_metadata_names() {
        let at = Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 0).unwrap();
        let record = conformance::sample_record("session", "message", at, 1, "text", b"raw");
        let entry = manifest_entry_for_raw_record(&record).unwrap();
        let source = HistorySourceManifest {
            source_kind: HistorySourceKind::Codex,
            source_instance_id: "codex-personal".to_owned(),
            cutover_cursor: "cursor:1".to_owned(),
            ownership: OwnershipAssignment::Personal {
                owner_id: "owner".to_owned(),
            },
            records: vec![entry.clone()],
        };
        let request = prepare_history_message_request(
            &DataSpaceId::new("space:personal"),
            &source,
            "owner",
            &entry,
            BlobRef::new("blob:sha256:raw"),
        )
        .unwrap();
        let canonical_json = request.event.observation.meta["canonical_json"]
            .as_str()
            .unwrap();
        let object_id = history_object_id("session", "message").unwrap();
        assert_eq!(
            request.event.observation.idempotency_key,
            history_v2_identity_key("codex-personal", &object_id, canonical_json).unwrap()
        );
        assert_eq!(
            request.event.observation.meta[HISTORY_SOURCE_INSTANCE_META],
            "codex-personal"
        );
        assert_eq!(
            request.event.observation.meta[HISTORY_OBJECT_ID_META],
            object_id
        );
        assert!(
            request
                .event
                .observation
                .meta
                .get("source_instance_id")
                .is_none()
        );
    }

    #[test]
    fn same_native_identity_with_changed_raw_is_rejected() {
        let at = Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 0).unwrap();
        let mut first = conformance::sample_record("session", "message", at, 1, "same", b"first");
        let mut second = first.clone();
        second.raw = b"second".to_vec();
        first.ordinal = 0;
        let request = HistoryInventoryRequest {
            inventory_id: "inventory:collision".to_owned(),
            data_space_id: DataSpaceId::new("space:personal"),
            captured_at: at,
            required_sources: vec![RequiredHistorySource {
                source_kind: HistorySourceKind::Codex,
                source_instance_id: "codex-personal".to_owned(),
            }],
            sources: vec![HistorySourceInventory {
                source_kind: HistorySourceKind::Codex,
                source_instance_id: "codex-personal".to_owned(),
                cutover_cursor: "2".to_owned(),
                ownership: OwnershipAssignment::Personal {
                    owner_id: "owner".to_owned(),
                },
                records: vec![first, second],
            }],
        };
        assert!(matches!(
            inventory_history(&request),
            Err(HistoryError::SourceIdentityCollision(_))
        ));
    }

    #[test]
    fn cross_source_native_overlap_blocks_import_without_text_deduplication() {
        let mut native = conformance::sample_record(
            "session",
            "native-occurrence",
            Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, 0).unwrap(),
            1,
            "same message",
            b"native raw",
        );
        for (key, value) in [
            (UPSTREAM_SOURCE_KIND_META, "claude_code"),
            (UPSTREAM_SOURCE_INSTANCE_META, "claude-code-personal"),
            (UPSTREAM_SESSION_META, "session"),
            (UPSTREAM_MESSAGE_META, "native-message"),
        ] {
            native.metadata.insert(key.to_owned(), value.to_owned());
        }
        let mut lake = native.clone();
        lake.source_message_id = "lethe-observation-id".to_owned();
        lake.raw = b"serialized LETHE observation".to_vec();
        let request = HistoryInventoryRequest {
            inventory_id: "inventory:overlap".to_owned(),
            data_space_id: DataSpaceId::new("space:personal"),
            captured_at: Utc.with_ymd_and_hms(2026, 7, 20, 1, 0, 0).unwrap(),
            required_sources: vec![
                RequiredHistorySource {
                    source_kind: HistorySourceKind::ClaudeCode,
                    source_instance_id: "claude-code-personal".to_owned(),
                },
                RequiredHistorySource {
                    source_kind: HistorySourceKind::Lethe,
                    source_instance_id: "lethe-personal".to_owned(),
                },
            ],
            sources: vec![
                HistorySourceInventory {
                    source_kind: HistorySourceKind::ClaudeCode,
                    source_instance_id: "claude-code-personal".to_owned(),
                    cutover_cursor: "native:1".to_owned(),
                    ownership: OwnershipAssignment::Personal {
                        owner_id: "owner".to_owned(),
                    },
                    records: vec![native],
                },
                HistorySourceInventory {
                    source_kind: HistorySourceKind::Lethe,
                    source_instance_id: "lethe-personal".to_owned(),
                    cutover_cursor: "lethe:1".to_owned(),
                    ownership: OwnershipAssignment::Personal {
                        owner_id: "owner".to_owned(),
                    },
                    records: vec![lake],
                },
            ],
        };
        let report = inventory_history(&request).unwrap();
        assert_eq!(report.cross_source_overlap_identities, 1);
        assert!(!report.ready_for_import);
        let root = std::env::temp_dir().join(format!("lethe-overlap-{}", uuid::Uuid::now_v7()));
        let store = SqliteOperationalEventStore::open(
            DataSpaceId::new("space:personal"),
            &root.join("overlap.sqlite3"),
            &root.join("blobs"),
            &[7; 32],
        )
        .unwrap();
        assert!(matches!(
            import_history(
                &store,
                &HistoryImportCommand {
                    inventory: request,
                    expected_manifest_digest: report.manifest.manifest_digest,
                    admission_generations: BTreeMap::new(),
                },
                1024,
            ),
            Err(HistoryError::CrossSourceOverlap(1))
        ));
        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn activation_handoff_fixture_covers_all_source_kinds_and_validates() {
        let handoff: HistoryActivationHandoff = serde_json::from_str(include_str!(
            "../tests/fixtures/history_activation_handoff.json"
        ))
        .unwrap();
        handoff.validate().unwrap();
        assert_eq!(handoff.sources.len(), 7);
        assert_eq!(
            serde_json::to_value(&handoff).unwrap(),
            serde_json::from_str::<serde_json::Value>(include_str!(
                "../tests/fixtures/history_activation_handoff.json"
            ))
            .unwrap()
        );
    }
}
