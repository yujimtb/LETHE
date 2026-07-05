use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::traits::ObservationDraft;
use lethe_core::domain::SemVer;
use serde_json::Value;

use crate::backbone::{BackboneItem, BackboneRecord, CodingAgentSourceConfig};

pub const CLAUDE_CODE_OBSERVER_ID: &str = "obs:claude-code-importer";
pub const CLAUDE_CODE_SOURCE_SYSTEM: &str = "sys:claude-code";
pub const CLAUDE_CODE_SOURCE_KEY: &str = "claude-code";

const CLAUDE_CODE_CONFIG: CodingAgentSourceConfig = CodingAgentSourceConfig {
    source_key: CLAUDE_CODE_SOURCE_KEY,
    observer_id: CLAUDE_CODE_OBSERVER_ID,
    source_system_id: CLAUDE_CODE_SOURCE_SYSTEM,
};

#[derive(Debug, Clone, Default)]
pub struct ClaudeCodeImportBatch {
    pub drafts: Vec<ObservationDraft>,
    pub audit: ClaudeCodeImportAudit,
}

#[derive(Debug, Clone, Default)]
pub struct ClaudeCodeImportAudit {
    pub files_read: usize,
    pub lines_read: usize,
    pub malformed_lines: Vec<ClaudeCodeAuditLine>,
    pub skipped_unknown_lines: Vec<ClaudeCodeAuditLine>,
    pub excluded_known_lines: usize,
    pub excluded_tool_result_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeCodeAuditLine {
    pub path: String,
    pub line: usize,
    pub reason: String,
}

pub struct ClaudeCodeImporter {
    adapter_version: SemVer,
}

impl ClaudeCodeImporter {
    pub fn new(adapter_version: SemVer) -> Self {
        Self { adapter_version }
    }

    pub fn import_archive_root(
        &self,
        archive_root: &Path,
    ) -> Result<ClaudeCodeImportBatch, AdapterError> {
        let source_root = archive_root.join("claude-code");
        if !source_root.is_dir() {
            return Err(malformed(format!(
                "archive root must contain claude-code directory: {}",
                archive_root.display()
            )));
        }

        let files = jsonl_files(&source_root)?;
        let mut batch = ClaudeCodeImportBatch::default();
        for file in files {
            let jsonl = fs::read_to_string(&file).map_err(|error| {
                AdapterError::Other(format!("failed to read {}: {error}", file.display()))
            })?;
            let label = file
                .strip_prefix(archive_root)
                .unwrap_or(file.as_path())
                .to_string_lossy()
                .replace('\\', "/");
            let imported = self.import_jsonl_str(&jsonl, &label)?;
            batch.audit.files_read += 1;
            batch.audit.lines_read += imported.audit.lines_read;
            batch
                .audit
                .malformed_lines
                .extend(imported.audit.malformed_lines);
            batch
                .audit
                .skipped_unknown_lines
                .extend(imported.audit.skipped_unknown_lines);
            batch.audit.excluded_known_lines += imported.audit.excluded_known_lines;
            batch.audit.excluded_tool_result_lines += imported.audit.excluded_tool_result_lines;
            batch.drafts.extend(imported.drafts);
        }
        Ok(batch)
    }

    pub fn import_jsonl_str(
        &self,
        jsonl: &str,
        source_path: &str,
    ) -> Result<ClaudeCodeImportBatch, AdapterError> {
        let mut batch = ClaudeCodeImportBatch::default();
        for (index, line) in jsonl.lines().enumerate() {
            let line_number = index + 1;
            if line.trim().is_empty() {
                continue;
            }
            batch.audit.lines_read += 1;

            let value = match serde_json::from_str::<Value>(line) {
                Ok(value) => value,
                Err(error) => {
                    batch.audit.malformed_lines.push(ClaudeCodeAuditLine {
                        path: source_path.to_owned(),
                        line: line_number,
                        reason: format!("MalformedTranscriptLine: {error}"),
                    });
                    continue;
                }
            };

            match map_row(value, line_number, source_path, &mut batch.audit) {
                Ok(Some(record)) => {
                    batch.drafts.push(
                        record.to_observation_draft(&CLAUDE_CODE_CONFIG, &self.adapter_version),
                    );
                }
                Ok(None) => {}
                Err(reason) => batch.audit.malformed_lines.push(ClaudeCodeAuditLine {
                    path: source_path.to_owned(),
                    line: line_number,
                    reason,
                }),
            }
        }
        Ok(batch)
    }
}

fn jsonl_files(root: &Path) -> Result<Vec<PathBuf>, AdapterError> {
    let mut files = Vec::new();
    collect_jsonl_files(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_jsonl_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), AdapterError> {
    for entry in fs::read_dir(path).map_err(|error| {
        AdapterError::Other(format!("failed to read {}: {error}", path.display()))
    })? {
        let entry = entry.map_err(|error| {
            AdapterError::Other(format!(
                "failed to read directory entry in {}: {error}",
                path.display()
            ))
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files(&path, files)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            files.push(path);
        }
    }
    Ok(())
}

fn map_row(
    row: Value,
    line_number: usize,
    source_path: &str,
    audit: &mut ClaudeCodeImportAudit,
) -> Result<Option<BackboneRecord>, String> {
    let top_type = string_at(&row, "/type");
    match top_type {
        Some("user") => map_user(row, audit),
        Some("assistant") => map_assistant(row),
        Some("tool_use") => map_top_level_tool_use(row),
        Some(kind) if is_known_metadata_type(kind) => {
            audit.excluded_known_lines += 1;
            Ok(None)
        }
        Some(kind) => {
            audit.skipped_unknown_lines.push(ClaudeCodeAuditLine {
                path: source_path.to_owned(),
                line: line_number,
                reason: format!("UnknownMessageType: {kind}"),
            });
            Ok(None)
        }
        None => {
            audit.skipped_unknown_lines.push(ClaudeCodeAuditLine {
                path: source_path.to_owned(),
                line: line_number,
                reason: "UnknownMessageType: <missing type>".to_owned(),
            });
            Ok(None)
        }
    }
}

fn map_user(
    row: Value,
    audit: &mut ClaudeCodeImportAudit,
) -> Result<Option<BackboneRecord>, String> {
    if bool_at(&row, "/isMeta").unwrap_or(false) {
        audit.excluded_known_lines += 1;
        return Ok(None);
    }
    let Some(message) = row.get("message") else {
        return Err("user line missing message".to_owned());
    };
    if string_at(message, "/role") != Some("user") {
        audit.excluded_known_lines += 1;
        return Ok(None);
    }
    let Some(content) = message.get("content") else {
        return Err("user message missing content".to_owned());
    };
    let (text, saw_tool_result) = user_text(content)?;
    let Some(text) = text else {
        if saw_tool_result {
            audit.excluded_tool_result_lines += 1;
        } else {
            audit.excluded_known_lines += 1;
        }
        return Ok(None);
    };

    Ok(Some(record_from_row(
        &row,
        BackboneItem::Message {
            role: "user".to_owned(),
            text,
        },
    )?))
}

fn map_assistant(row: Value) -> Result<Option<BackboneRecord>, String> {
    let Some(message) = row.get("message") else {
        return Err("assistant line missing message".to_owned());
    };
    if string_at(message, "/role") != Some("assistant") {
        return Ok(None);
    }
    let Some(content) = message.get("content") else {
        return Err("assistant message missing content".to_owned());
    };
    let (text, tool_calls) = assistant_backbone(content)?;
    if !tool_calls.is_empty() {
        return Ok(Some(record_from_row(
            &row,
            BackboneItem::ToolCall {
                tool_name: tool_calls
                    .iter()
                    .map(|call| call.tool_name.as_str())
                    .collect::<Vec<_>>()
                    .join("+"),
                references: merge_tool_references(tool_calls),
            },
        )?));
    }
    let Some(text) = text else {
        return Ok(None);
    };
    Ok(Some(record_from_row(
        &row,
        BackboneItem::Message {
            role: "assistant".to_owned(),
            text,
        },
    )?))
}

fn map_top_level_tool_use(row: Value) -> Result<Option<BackboneRecord>, String> {
    let tool_name = required_string(&row, "/name")?;
    let references = tool_references(row.get("input").unwrap_or(&Value::Null));
    Ok(Some(record_from_row(
        &row,
        BackboneItem::ToolCall {
            tool_name,
            references,
        },
    )?))
}

fn record_from_row(row: &Value, item: BackboneItem) -> Result<BackboneRecord, String> {
    let session_id = required_string(row, "/sessionId")?;
    let message_uuid = required_string(row, "/uuid")?;
    let parent_message_id = optional_string(row, "/parentUuid");
    let is_sidechain = bool_at(row, "/isSidechain").unwrap_or(false);
    let parent_thread_id = optional_string(row, "/parentSessionId")
        .or_else(|| optional_string(row, "/parent_session_id"));

    Ok(BackboneRecord {
        session_id: session_id.clone(),
        transcript_id: session_id.clone(),
        parent_message_id,
        is_sidechain,
        parent_thread_id,
        thread_source: if is_sidechain {
            "sidechain".to_owned()
        } else {
            "main".to_owned()
        },
        object_id: format!("{session_id}:{message_uuid}"),
        published: row_timestamp(row)?,
        item,
    })
}

fn user_text(content: &Value) -> Result<(Option<String>, bool), String> {
    match content {
        Value::String(text) => Ok((non_blank_join(vec![text.clone()]), false)),
        Value::Array(items) => {
            let mut parts = Vec::new();
            let mut saw_tool_result = false;
            for item in items {
                match string_at(item, "/type") {
                    Some("text") => {
                        if let Some(text) = string_at(item, "/text") {
                            parts.push(text.to_owned());
                        }
                    }
                    Some("tool_result") => saw_tool_result = true,
                    Some(_) | None => {}
                }
            }
            Ok((non_blank_join(parts), saw_tool_result))
        }
        _ => Err("user content must be string or array".to_owned()),
    }
}

#[derive(Debug, Clone)]
struct ParsedToolCall {
    tool_name: String,
    references: BTreeMap<String, Value>,
}

fn assistant_backbone(content: &Value) -> Result<(Option<String>, Vec<ParsedToolCall>), String> {
    match content {
        Value::String(text) => Ok((non_blank_join(vec![text.clone()]), Vec::new())),
        Value::Array(items) => {
            let mut parts = Vec::new();
            let mut tool_calls = Vec::new();
            for item in items {
                match string_at(item, "/type") {
                    Some("text") => {
                        if let Some(text) = string_at(item, "/text") {
                            parts.push(text.to_owned());
                        }
                    }
                    Some("tool_use") => tool_calls.push(ParsedToolCall {
                        tool_name: required_string(item, "/name")?,
                        references: tool_references(item.get("input").unwrap_or(&Value::Null)),
                    }),
                    Some("thinking" | "redacted_thinking") => {}
                    Some(_) | None => {}
                }
            }
            Ok((non_blank_join(parts), tool_calls))
        }
        _ => Err("assistant content must be string or array".to_owned()),
    }
}

fn merge_tool_references(tool_calls: Vec<ParsedToolCall>) -> BTreeMap<String, Value> {
    let mut merged = BTreeMap::new();
    for (index, call) in tool_calls.iter().enumerate() {
        merged.insert(
            format!("tool_{index}_name"),
            Value::String(call.tool_name.clone()),
        );
        for (key, value) in &call.references {
            merged.insert(format!("tool_{index}_{key}"), value.clone());
        }
    }
    merged
}

fn tool_references(input: &Value) -> BTreeMap<String, Value> {
    let mut references = BTreeMap::new();
    let Some(object) = input.as_object() else {
        return references;
    };
    for (key, value) in object {
        if is_reference_key(key) && safe_reference_value(value) {
            references.insert(key.clone(), value.clone());
        }
    }
    references
}

fn is_reference_key(key: &str) -> bool {
    matches!(
        key,
        "file_path"
            | "file_paths"
            | "path"
            | "paths"
            | "pattern"
            | "patterns"
            | "glob"
            | "query"
            | "url"
            | "urls"
            | "notebook_path"
            | "old_path"
            | "new_path"
            | "relative_path"
    )
}

fn safe_reference_value(value: &Value) -> bool {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => true,
        Value::Array(items) => items.iter().all(|item| {
            matches!(
                item,
                Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null
            )
        }),
        Value::Object(_) => false,
    }
}

fn row_timestamp(row: &Value) -> Result<DateTime<Utc>, String> {
    let raw = required_string(row, "/timestamp")?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|value| value.to_utc())
        .map_err(|error| format!("timestamp must be RFC3339: {error}"))
}

fn is_known_metadata_type(line_type: &str) -> bool {
    matches!(
        line_type,
        "ai-title"
            | "attachment"
            | "file-history-snapshot"
            | "last-prompt"
            | "mode"
            | "permission-mode"
            | "queue-operation"
            | "system"
    )
}

fn required_string(value: &Value, pointer: &'static str) -> Result<String, String> {
    optional_string(value, pointer)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{pointer} must be a non-empty string"))
}

fn optional_string(value: &Value, pointer: &'static str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn string_at<'a>(value: &'a Value, pointer: &'static str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

fn bool_at(value: &Value, pointer: &'static str) -> Option<bool> {
    value.pointer(pointer).and_then(Value::as_bool)
}

fn non_blank_join(values: Vec<String>) -> Option<String> {
    let joined = values
        .into_iter()
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!joined.trim().is_empty()).then_some(joined)
}

fn malformed(message: String) -> AdapterError {
    AdapterError::MalformedResponse { message }
}

#[cfg(test)]
mod tests {
    use lethe_adapter_api::idempotency::{CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY};
    use lethe_core::domain::*;
    use lethe_engine::lake::{BlobStore, IngestRequest, IngestionGate, LakeStore};
    use lethe_registry::registry::*;

    use super::*;

    const SECRET_ENV_VALUE: &str = "DATABASE_URL=postgres://secret@example.test/db";

    fn importer() -> ClaudeCodeImporter {
        ClaudeCodeImporter::new(SemVer::new("1.0.0"))
    }

    fn fixture() -> String {
        [
            serde_json::json!({"type":"mode","mode":"normal","sessionId":"session-1"}).to_string(),
            serde_json::json!({
                "type": "user",
                "uuid": "user-1",
                "parentUuid": null,
                "isSidechain": false,
                "message": {"role": "user", "content": "Please inspect .env without leaking values."},
                "timestamp": "2026-06-01T00:00:00.000Z",
                "sessionId": "session-1",
                "cwd": "D:/repo",
                "version": "2.1.170"
            })
            .to_string(),
            serde_json::json!({
                "type": "assistant",
                "uuid": "assistant-tool-1",
                "parentUuid": "user-1",
                "isSidechain": true,
                "parentSessionId": "parent-session-1",
                "message": {
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": "toolu-read-env",
                        "name": "Read",
                        "input": {"file_path": ".env", "limit": 200}
                    }]
                },
                "timestamp": "2026-06-01T00:00:01.000Z",
                "sessionId": "session-1"
            })
            .to_string(),
            serde_json::json!({
                "type": "user",
                "uuid": "tool-result-1",
                "parentUuid": "assistant-tool-1",
                "isSidechain": true,
                "message": {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "toolu-read-env",
                        "content": SECRET_ENV_VALUE
                    }]
                },
                "toolUseResult": {"content": SECRET_ENV_VALUE},
                "timestamp": "2026-06-01T00:00:02.000Z",
                "sessionId": "session-1"
            })
            .to_string(),
            serde_json::json!({
                "type": "assistant",
                "uuid": "assistant-text-1",
                "parentUuid": "tool-result-1",
                "isSidechain": true,
                "parentSessionId": "parent-session-1",
                "message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "I checked the file path and will not repeat values."}]
                },
                "timestamp": "2026-06-01T00:00:03.000Z",
                "sessionId": "session-1"
            })
            .to_string(),
            serde_json::json!({
                "type": "assistant",
                "uuid": "assistant-bash-1",
                "parentUuid": "assistant-text-1",
                "isSidechain": false,
                "message": {
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": "toolu-bash",
                        "name": "Bash",
                        "input": {"command": "cat .env", "description": "read env"}
                    }]
                },
                "timestamp": "2026-06-01T00:00:04.000Z",
                "sessionId": "session-1"
            })
            .to_string(),
            serde_json::json!({"type":"future-shape","sessionId":"session-1"}).to_string(),
            "{not-json".to_owned(),
        ]
        .join("\n")
    }

    #[test]
    fn parses_real_shape_fixture_and_skips_broken_lines() {
        let batch = importer()
            .import_jsonl_str(&fixture(), "claude-code/projects/session-1.jsonl")
            .unwrap();

        assert_eq!(batch.drafts.len(), 4);
        assert_eq!(batch.audit.excluded_known_lines, 1);
        assert_eq!(batch.audit.excluded_tool_result_lines, 1);
        assert_eq!(batch.audit.skipped_unknown_lines.len(), 1);
        assert_eq!(batch.audit.malformed_lines.len(), 1);
        assert!(
            batch.audit.skipped_unknown_lines[0]
                .reason
                .contains("UnknownMessageType")
        );
        assert!(
            batch.audit.malformed_lines[0]
                .reason
                .contains("MalformedTranscriptLine")
        );
    }

    #[test]
    fn env_tool_result_content_never_enters_canonical() {
        let batch = importer()
            .import_jsonl_str(&fixture(), "fixture.jsonl")
            .unwrap();
        let canonical = batch
            .drafts
            .iter()
            .map(|draft| draft.meta[CANONICAL_JSON_META_KEY].as_str().unwrap())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(!canonical.contains(SECRET_ENV_VALUE));
        assert!(!canonical.contains("postgres://secret"));
        assert!(!canonical.contains("cat .env"));
        assert!(canonical.contains("Read"));
        assert!(canonical.contains(".env"));
    }

    #[test]
    fn identity_key_and_published_use_source_message_fields() {
        let batch = importer()
            .import_jsonl_str(&fixture(), "fixture.jsonl")
            .unwrap();
        let first = &batch.drafts[0];

        assert!(
            first
                .idempotency_key
                .as_str()
                .starts_with("claude-code:session-1:user-1:")
        );
        assert_eq!(first.published.to_rfc3339(), "2026-06-01T00:00:00+00:00");
        assert_eq!(first.meta[OBJECT_ID_META_KEY], "session-1:user-1");
    }

    #[test]
    fn sidechain_parent_metadata_is_preserved() {
        let batch = importer()
            .import_jsonl_str(&fixture(), "fixture.jsonl")
            .unwrap();
        let tool = batch
            .drafts
            .iter()
            .find(|draft| draft.payload["item"]["kind"] == "tool_call")
            .unwrap();

        assert_eq!(tool.payload["is_sidechain"], true);
        assert_eq!(tool.payload["parent_thread_id"], "parent-session-1");
        assert_eq!(tool.payload["parent_message_id"], "user-1");
        assert_eq!(tool.meta["thread_source"], "sidechain");
        assert_eq!(tool.meta["parent_thread_id"], "parent-session-1");
    }

    #[test]
    fn same_archive_snapshot_reingest_is_all_duplicate() {
        let batch = importer()
            .import_jsonl_str(&fixture(), "fixture.jsonl")
            .unwrap();
        let registry = setup_registry();
        let mut lake = LakeStore::new();
        let blobs = BlobStore::new();

        let first = ingest_all(&registry, &mut lake, &blobs, batch.drafts.clone());
        let second = ingest_all(&registry, &mut lake, &blobs, batch.drafts);

        assert_eq!(first, (4, 0, 0));
        assert_eq!(second, (0, 4, 0));
        assert_eq!(lake.len(), 4);
    }

    fn ingest_all(
        registry: &RegistryStore,
        lake: &mut LakeStore,
        blobs: &BlobStore,
        drafts: Vec<ObservationDraft>,
    ) -> (usize, usize, usize) {
        let mut ingested = 0;
        let mut duplicates = 0;
        let mut quarantined = 0;
        for draft in drafts {
            let mut gate = IngestionGate {
                registry,
                lake,
                blobs,
            };
            match gate.ingest(IngestRequest {
                schema: draft.schema,
                schema_version: draft.schema_version,
                observer: draft.observer,
                source_system: draft.source_system,
                authority_model: draft.authority_model,
                capture_model: draft.capture_model,
                subject: draft.subject,
                target: draft.target,
                payload: draft.payload,
                attachments: draft.attachments,
                published: draft.published,
                idempotency_key: draft.idempotency_key,
                meta: draft.meta,
            }) {
                IngestResult::Ingested { .. } => ingested += 1,
                IngestResult::Duplicate { .. } => duplicates += 1,
                IngestResult::Quarantined { .. } => quarantined += 1,
                IngestResult::Rejected { message, .. } => panic!("{message}"),
            }
        }
        (ingested, duplicates, quarantined)
    }

    fn setup_registry() -> RegistryStore {
        let mut reg = RegistryStore::new();
        reg.register_source_system(SourceSystem {
            id: SourceSystemRef::new(CLAUDE_CODE_SOURCE_SYSTEM),
            name: "Claude Code".into(),
            provider: Some("Anthropic".into()),
            api_version: None,
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
        reg.register_observer(Observer {
            id: ObserverRef::new(CLAUDE_CODE_OBSERVER_ID),
            name: "Claude Code Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new(CLAUDE_CODE_SOURCE_SYSTEM),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new(
                super::super::backbone::CODING_AGENT_MESSAGE_SCHEMA,
            )],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
        reg.register_schema(ObservationSchema {
            id: SchemaRef::new(super::super::backbone::CODING_AGENT_MESSAGE_SCHEMA),
            name: "Coding Agent Message".into(),
            version: SemVer::new(super::super::backbone::CODING_AGENT_MESSAGE_SCHEMA_VERSION),
            subject_type: EntityTypeRef::new("et:message"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        })
        .unwrap();
        reg
    }
}
