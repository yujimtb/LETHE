use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::traits::ObservationDraft;
use lethe_core::domain::SemVer;
use serde_json::Value;

use crate::backbone::{
    BackboneHistoryRecord, BackboneItem, BackboneRecord, CodingAgentSourceConfig,
};

pub const CODEX_OBSERVER_ID: &str = "obs:codex-importer";
pub const CODEX_SOURCE_SYSTEM: &str = "sys:codex";
pub const CODEX_SOURCE_KEY: &str = "codex";

const CODEX_CONFIG: CodingAgentSourceConfig = CodingAgentSourceConfig {
    source_key: CODEX_SOURCE_KEY,
    observer_id: CODEX_OBSERVER_ID,
    source_system_id: CODEX_SOURCE_SYSTEM,
};

#[derive(Debug, Clone, Default)]
pub struct CodingAgentImportBatch {
    pub drafts: Vec<ObservationDraft>,
    pub history_records: Vec<BackboneHistoryRecord>,
    pub audit: ImportAudit,
}

#[derive(Debug, Clone, Default)]
pub struct ImportAudit {
    pub files_read: usize,
    pub transcripts_read: usize,
    pub malformed_lines: Vec<AuditLine>,
    pub skipped_unknown_lines: Vec<AuditLine>,
    pub excluded_known_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditLine {
    pub path: String,
    pub line: usize,
    pub reason: String,
}

#[derive(Debug, Clone)]
struct CodexSessionMeta {
    session_id: String,
    transcript_id: String,
    parent_thread_id: Option<String>,
    thread_source: String,
}

pub struct CodexImporter {
    adapter_version: SemVer,
}

impl CodexImporter {
    pub fn new(adapter_version: SemVer) -> Self {
        Self { adapter_version }
    }

    pub fn import_archive_path(&self, path: &Path) -> Result<CodingAgentImportBatch, AdapterError> {
        let session_roots = resolve_session_roots(path)?;
        let mut files = Vec::new();
        for root in &session_roots {
            files.extend(jsonl_files(root)?);
        }
        files.sort();
        files.dedup();
        if files.is_empty() {
            return Err(malformed(format!(
                "Codex native root contains no jsonl files: {}",
                path.display()
            )));
        }

        let mut batch = CodingAgentImportBatch::default();
        for file in files {
            let jsonl = fs::read_to_string(&file).map_err(|error| {
                AdapterError::Other(format!("failed to read {}: {error}", file.display()))
            })?;
            let label = file
                .strip_prefix(path)
                .unwrap_or(file.as_path())
                .to_string_lossy()
                .replace('\\', "/");
            let imported = self.import_jsonl_str(&jsonl, &label)?;
            batch.audit.files_read += 1;
            batch.audit.transcripts_read += imported.audit.transcripts_read;
            batch
                .audit
                .malformed_lines
                .extend(imported.audit.malformed_lines);
            batch
                .audit
                .skipped_unknown_lines
                .extend(imported.audit.skipped_unknown_lines);
            batch.audit.excluded_known_lines += imported.audit.excluded_known_lines;
            batch.drafts.extend(imported.drafts);
            batch.history_records.extend(imported.history_records);
        }
        Ok(batch)
    }

    pub fn visit_native_path<F>(
        &self,
        path: &Path,
        max_record_bytes: usize,
        mut visitor: F,
    ) -> Result<ImportAudit, AdapterError>
    where
        F: FnMut(BackboneHistoryRecord) -> Result<(), AdapterError>,
    {
        if max_record_bytes == 0 {
            return Err(malformed("max_record_bytes must be positive".to_owned()));
        }
        let session_roots = resolve_session_roots(path)?;
        let mut files = Vec::new();
        for root in &session_roots {
            files.extend(jsonl_files(root)?);
        }
        files.sort();
        files.dedup();
        if files.is_empty() {
            return Err(malformed(format!(
                "Codex native root contains no jsonl files: {}",
                path.display()
            )));
        }
        let mut audit = ImportAudit::default();
        for file in files {
            let label = file
                .strip_prefix(path)
                .unwrap_or(file.as_path())
                .to_string_lossy()
                .replace('\\', "/");
            let meta = find_session_meta(&file, &label, max_record_bytes)?;
            let reader = BufReader::new(fs::File::open(&file).map_err(|error| {
                AdapterError::Other(format!("failed to read {}: {error}", file.display()))
            })?);
            audit.files_read += 1;
            audit.transcripts_read += 1;
            for (index, line) in reader.split(b'\n').enumerate() {
                let line_number = index + 1;
                let mut raw = line.map_err(|error| {
                    AdapterError::Other(format!("failed to read {}: {error}", file.display()))
                })?;
                if raw.last() == Some(&b'\r') {
                    raw.pop();
                }
                if raw.is_empty() {
                    continue;
                }
                if raw.len() > max_record_bytes {
                    return Err(malformed(format!(
                        "Codex record exceeds max_record_bytes at {label}:{line_number}: {} > {max_record_bytes}",
                        raw.len()
                    )));
                }
                let row = match serde_json::from_slice::<Value>(&raw) {
                    Ok(value) => value,
                    Err(error) => {
                        audit.malformed_lines.push(AuditLine {
                            path: label.clone(),
                            line: line_number,
                            reason: format!("MalformedTranscriptLine: {error}"),
                        });
                        continue;
                    }
                };
                match map_row(row, line_number, &label, &meta, &mut audit) {
                    Ok(Some(record)) => visitor(BackboneHistoryRecord {
                        record,
                        source_path: label.clone(),
                        line_number,
                        raw,
                    })?,
                    Ok(None) => {}
                    Err(reason) => audit.malformed_lines.push(AuditLine {
                        path: label.clone(),
                        line: line_number,
                        reason,
                    }),
                }
            }
        }
        Ok(audit)
    }

    pub fn import_jsonl_str(
        &self,
        jsonl: &str,
        source_path: &str,
    ) -> Result<CodingAgentImportBatch, AdapterError> {
        let mut rows = Vec::new();
        let mut audit = ImportAudit::default();
        for (index, line) in jsonl.lines().enumerate() {
            let line_number = index + 1;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(line) {
                Ok(value) => rows.push((line_number, value, line.as_bytes().to_vec())),
                Err(error) => audit.malformed_lines.push(AuditLine {
                    path: source_path.to_owned(),
                    line: line_number,
                    reason: format!("MalformedTranscriptLine: {error}"),
                }),
            }
        }

        let meta = rows
            .iter()
            .find_map(|(_, row, _)| {
                (string_at(row, "/type") == Some("session_meta")).then(|| parse_session_meta(row))
            })
            .transpose()?
            .ok_or_else(|| {
                malformed(format!(
                    "codex transcript has no session_meta: {source_path}"
                ))
            })?;

        let mut drafts = Vec::new();
        let mut history_records = Vec::new();
        for (line_number, row, raw) in rows {
            match map_row(row, line_number, source_path, &meta, &mut audit) {
                Ok(Some(record)) => {
                    drafts.push(record.to_observation_draft(&CODEX_CONFIG, &self.adapter_version));
                    history_records.push(BackboneHistoryRecord {
                        record,
                        source_path: source_path.to_owned(),
                        line_number,
                        raw,
                    });
                }
                Ok(None) => {}
                Err(reason) => audit.malformed_lines.push(AuditLine {
                    path: source_path.to_owned(),
                    line: line_number,
                    reason,
                }),
            }
        }

        audit.transcripts_read = 1;
        Ok(CodingAgentImportBatch {
            drafts,
            history_records,
            audit,
        })
    }
}

fn find_session_meta(
    path: &Path,
    source_path: &str,
    max_record_bytes: usize,
) -> Result<CodexSessionMeta, AdapterError> {
    let reader = BufReader::new(fs::File::open(path).map_err(|error| {
        AdapterError::Other(format!("failed to read {}: {error}", path.display()))
    })?);
    for (index, line) in reader.split(b'\n').enumerate() {
        let mut raw = line.map_err(|error| {
            AdapterError::Other(format!("failed to read {}: {error}", path.display()))
        })?;
        if raw.last() == Some(&b'\r') {
            raw.pop();
        }
        if raw.is_empty() {
            continue;
        }
        if raw.len() > max_record_bytes {
            return Err(malformed(format!(
                "Codex record exceeds max_record_bytes at {source_path}:{}: {} > {max_record_bytes}",
                index + 1,
                raw.len()
            )));
        }
        let Ok(row) = serde_json::from_slice::<Value>(&raw) else {
            continue;
        };
        if string_at(&row, "/type") == Some("session_meta") {
            return parse_session_meta(&row);
        }
    }
    Err(malformed(format!(
        "codex transcript has no session_meta: {source_path}"
    )))
}

fn resolve_session_roots(path: &Path) -> Result<Vec<PathBuf>, AdapterError> {
    if !path.is_dir() {
        return Err(malformed(format!(
            "--archive must point to a directory: {}",
            path.display()
        )));
    }
    let archive_codex_sessions = path.join("codex").join("sessions");
    if archive_codex_sessions.is_dir() {
        let mut roots = vec![archive_codex_sessions];
        let archived = path.join("codex").join("archived_sessions");
        if archived.is_dir() {
            roots.push(archived);
        }
        return Ok(roots);
    }
    let direct_sessions = path.join("sessions");
    if direct_sessions.is_dir() {
        let mut roots = vec![direct_sessions];
        let archived = path.join("archived_sessions");
        if archived.is_dir() {
            roots.push(archived);
        }
        return Ok(roots);
    }
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "sessions")
    {
        return Ok(vec![path.to_path_buf()]);
    }
    Err(malformed(format!(
        "Codex path must be an archive root containing codex/sessions, a native root containing sessions (and optional archived_sessions), or a sessions directory: {}",
        path.display()
    )))
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

fn parse_session_meta(row: &Value) -> Result<CodexSessionMeta, AdapterError> {
    let payload = row
        .get("payload")
        .ok_or_else(|| malformed("session_meta missing payload".to_owned()))?;
    if optional_string(payload, "/session_id").is_none()
        && optional_string(payload, "/parent_thread_id").is_none()
    {
        let transcript_id = required_string(payload, "/id")?;
        let thread_source =
            optional_string(payload, "/thread_source").unwrap_or_else(|| "legacy-main".to_owned());
        return Ok(CodexSessionMeta {
            session_id: transcript_id.clone(),
            transcript_id,
            parent_thread_id: None,
            thread_source,
        });
    }
    Ok(CodexSessionMeta {
        session_id: required_string(payload, "/session_id")?,
        transcript_id: required_string(payload, "/id")?,
        parent_thread_id: optional_string(payload, "/parent_thread_id"),
        thread_source: session_thread_source(payload)?,
    })
}

fn session_thread_source(payload: &Value) -> Result<String, AdapterError> {
    if let Some(thread_source) =
        optional_string(payload, "/thread_source").filter(|value| !value.trim().is_empty())
    {
        return Ok(thread_source);
    }
    if optional_string(payload, "/parent_thread_id")
        .filter(|value| !value.trim().is_empty())
        .is_none()
        && string_at(payload, "/source") == Some("vscode")
    {
        return Ok("user".to_owned());
    }
    Err(malformed(
        "/thread_source must be a non-empty string".to_owned(),
    ))
}

fn map_row(
    row: Value,
    line_number: usize,
    source_path: &str,
    meta: &CodexSessionMeta,
    audit: &mut ImportAudit,
) -> Result<Option<BackboneRecord>, String> {
    let top_type = string_at(&row, "/type");
    if top_type == Some("session_meta") {
        audit.excluded_known_lines += 1;
        return Ok(None);
    }
    if matches!(top_type, Some("event_msg" | "turn_context")) {
        audit.excluded_known_lines += 1;
        return Ok(None);
    }
    if top_type != Some("response_item") {
        audit.skipped_unknown_lines.push(AuditLine {
            path: source_path.to_owned(),
            line: line_number,
            reason: format!(
                "UnknownMessageType: {}",
                top_type.unwrap_or("<missing type>")
            ),
        });
        return Ok(None);
    }

    let payload = row
        .get("payload")
        .ok_or_else(|| "response_item missing payload".to_owned())?;
    match string_at(payload, "/type") {
        Some("message") => {
            let role = string_at(payload, "/role")
                .ok_or_else(|| "message payload missing role".to_owned())?;
            if role != "user" && role != "assistant" {
                audit.excluded_known_lines += 1;
                return Ok(None);
            }
            map_message(payload, &row, line_number, meta).map(Some)
        }
        Some("function_call") => map_function_call(payload, &row, line_number, meta).map(Some),
        Some("function_call_output" | "reasoning") => {
            audit.excluded_known_lines += 1;
            Ok(None)
        }
        Some(kind) => {
            audit.skipped_unknown_lines.push(AuditLine {
                path: source_path.to_owned(),
                line: line_number,
                reason: format!("UnknownMessageType: response_item:{kind}"),
            });
            Ok(None)
        }
        None => Err("response_item payload missing type".to_owned()),
    }
}

fn map_message(
    payload: &Value,
    row: &Value,
    line_number: usize,
    meta: &CodexSessionMeta,
) -> Result<BackboneRecord, String> {
    let role = string_at(payload, "/role")
        .ok_or_else(|| "message payload missing role".to_owned())?
        .to_owned();
    let text = message_text(payload, &role)
        .ok_or_else(|| format!("message role {role} has no textual content"))?;
    let item_key = optional_string(payload, "/id").unwrap_or_else(|| format!("line-{line_number}"));
    Ok(BackboneRecord {
        session_id: meta.session_id.clone(),
        transcript_id: meta.transcript_id.clone(),
        parent_thread_id: meta.parent_thread_id.clone(),
        thread_source: meta.thread_source.clone(),
        object_id: format!("{}:{item_key}", meta.transcript_id),
        published: row_timestamp(row)?,
        parent_message_id: None,
        is_sidechain: meta.thread_source == "subagent",
        item: BackboneItem::Message { role, text },
    })
}

fn map_function_call(
    payload: &Value,
    row: &Value,
    line_number: usize,
    meta: &CodexSessionMeta,
) -> Result<BackboneRecord, String> {
    let tool_name = string_at(payload, "/name")
        .ok_or_else(|| "function_call missing name".to_owned())?
        .to_owned();
    let item_key = optional_string(payload, "/id")
        .or_else(|| optional_string(payload, "/call_id"))
        .unwrap_or_else(|| format!("line-{line_number}"));
    let references = payload
        .get("arguments")
        .and_then(Value::as_str)
        .map(tool_references_from_arguments)
        .unwrap_or_default();
    Ok(BackboneRecord {
        session_id: meta.session_id.clone(),
        transcript_id: meta.transcript_id.clone(),
        parent_thread_id: meta.parent_thread_id.clone(),
        thread_source: meta.thread_source.clone(),
        object_id: format!("{}:{item_key}", meta.transcript_id),
        published: row_timestamp(row)?,
        parent_message_id: None,
        is_sidechain: meta.thread_source == "subagent",
        item: BackboneItem::ToolCall {
            tool_name,
            references,
        },
    })
}

fn row_timestamp(row: &Value) -> Result<DateTime<Utc>, String> {
    let raw = string_at(row, "/timestamp").ok_or_else(|| "row missing timestamp".to_owned())?;
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.to_utc())
        .map_err(|error| format!("timestamp must be RFC3339: {error}"))
}

fn message_text(payload: &Value, role: &str) -> Option<String> {
    let expected_item_type = if role == "assistant" {
        "output_text"
    } else {
        "input_text"
    };
    let parts = payload
        .get("content")?
        .as_array()?
        .iter()
        .filter_map(|item| {
            (string_at(item, "/type") == Some(expected_item_type))
                .then(|| string_at(item, "/text"))
                .flatten()
        })
        .filter(|text| !text.trim().is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn tool_references_from_arguments(arguments: &str) -> BTreeMap<String, Value> {
    let Ok(value) = serde_json::from_str::<Value>(arguments) else {
        return BTreeMap::new();
    };
    let mut references = BTreeMap::new();
    let Some(object) = value.as_object() else {
        return references;
    };
    for (key, value) in object {
        if is_reference_key(key) && safe_reference_value(value) {
            references.insert(key.clone(), value.clone());
        } else if key == "tool_uses" {
            let names = parallel_tool_names(value);
            if !names.is_empty() {
                references.insert("parallel_tools".to_owned(), serde_json::json!(names));
            }
        }
    }
    references
}

fn parallel_tool_names(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| string_at(entry, "/recipient_name").map(ToOwned::to_owned))
        .collect()
}

fn is_reference_key(key: &str) -> bool {
    matches!(
        key,
        "path"
            | "paths"
            | "file"
            | "files"
            | "filename"
            | "filenames"
            | "directory"
            | "dir"
            | "root"
            | "cwd"
            | "workdir"
            | "repository"
            | "repository_full_name"
            | "repo"
            | "base"
            | "head"
            | "branch"
            | "ref"
            | "ref_id"
            | "line"
            | "line_number"
            | "pattern"
            | "glob"
            | "session_id"
            | "channel"
            | "channel_id"
            | "thread_ts"
            | "message_ts"
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

fn required_string(value: &Value, pointer: &'static str) -> Result<String, AdapterError> {
    optional_string(value, pointer)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| malformed(format!("{pointer} must be a non-empty string")))
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

fn malformed(message: String) -> AdapterError {
    AdapterError::MalformedResponse { message }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use lethe_adapter_api::idempotency::{CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY};

    use super::*;

    fn importer() -> CodexImporter {
        CodexImporter::new(SemVer::new("1.0.0"))
    }

    fn fixture() -> String {
        [
            serde_json::json!({
                "timestamp": "2026-07-05T00:00:00.000Z",
                "type": "session_meta",
                "payload": {
                    "session_id": "parent-session",
                    "id": "transcript-main",
                    "timestamp": "2026-07-05T00:00:00.000Z",
                    "cwd": "D:\\repo",
                    "originator": "codex-tui",
                    "source": "cli",
                    "thread_source": "user"
                }
            })
            .to_string(),
            serde_json::json!({
                "timestamp": "2026-07-05T00:00:01.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "read the env file"}]
                }
            })
            .to_string(),
            serde_json::json!({
                "timestamp": "2026-07-05T00:00:02.000Z",
                "type": "response_item",
                "payload": {
                    "type": "function_call",
                    "id": "fc-1",
                    "name": "shell_command",
                    "call_id": "call-1",
                    "arguments": "{\"command\":\"Get-Content .env\",\"workdir\":\"D:\\\\repo\"}"
                }
            })
            .to_string(),
            serde_json::json!({
                "timestamp": "2026-07-05T00:00:03.000Z",
                "type": "response_item",
                "payload": {
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": "API_TOKEN=super-secret"
                }
            })
            .to_string(),
            serde_json::json!({
                "timestamp": "2026-07-05T00:00:04.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": "msg-1",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "I checked the path."}]
                }
            })
            .to_string(),
        ]
        .join("\n")
    }

    #[test]
    fn codex_fixture_excludes_tool_results_and_argument_body_from_canonical() {
        let batch = importer()
            .import_jsonl_str(&fixture(), "fixture.jsonl")
            .unwrap();

        assert_eq!(batch.drafts.len(), 3);
        let serialized = serde_json::to_string(&batch.drafts).unwrap();
        for forbidden in ["API_TOKEN=super-secret", "super-secret", "Get-Content .env"] {
            assert!(
                !serialized.contains(forbidden),
                "forbidden transcript content leaked: {forbidden}"
            );
        }
        let tool = batch
            .drafts
            .iter()
            .find(|draft| draft.payload["item"]["kind"] == "tool_call")
            .unwrap();
        assert!(tool.payload["item"]["references"].get("command").is_none());
        assert!(serialized.contains("shell_command"));
        assert!(serialized.contains("D:\\\\repo"));
    }

    #[test]
    fn codex_import_is_deterministic_and_uses_message_timestamps() {
        let first = importer()
            .import_jsonl_str(&fixture(), "fixture.jsonl")
            .unwrap();
        let second = importer()
            .import_jsonl_str(&fixture(), "fixture.jsonl")
            .unwrap();
        let first_keys = first
            .drafts
            .iter()
            .map(|draft| draft.idempotency_key.as_str().to_owned())
            .collect::<Vec<_>>();
        let second_keys = second
            .drafts
            .iter()
            .map(|draft| draft.idempotency_key.as_str().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(first_keys, second_keys);
        assert_eq!(
            first.drafts[0].published.to_rfc3339(),
            "2026-07-05T00:00:01+00:00"
        );
        assert_eq!(
            first.drafts[0].idempotency_key.as_str().split(':').next(),
            Some("codex")
        );
        assert_eq!(
            first.drafts[0].meta[OBJECT_ID_META_KEY],
            "transcript-main:line-2"
        );
        assert!(
            first.drafts[0].meta[CANONICAL_JSON_META_KEY]
                .as_str()
                .unwrap()
                .contains("read the env file")
        );
    }

    #[test]
    fn codex_subagent_metadata_is_preserved() {
        let jsonl = serde_json::json!({
            "timestamp": "2026-07-05T00:00:00.000Z",
            "type": "session_meta",
            "payload": {
                "session_id": "parent-session",
                "id": "child-transcript",
                "parent_thread_id": "parent-session",
                "timestamp": "2026-07-05T00:00:00.000Z",
                "cwd": "D:\\repo",
                "originator": "codex-tui",
                "source": {"subagent": {}},
                "thread_source": "subagent"
            }
        })
        .to_string()
            + "\n"
            + &serde_json::json!({
                "timestamp": "2026-07-05T00:00:01.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": "msg-child",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "sub conclusion"}]
                }
            })
            .to_string();

        let batch = importer().import_jsonl_str(&jsonl, "sub.jsonl").unwrap();
        let draft = &batch.drafts[0];

        assert_eq!(draft.payload["session_id"], "parent-session");
        assert_eq!(draft.payload["transcript_id"], "child-transcript");
        assert_eq!(draft.payload["thread_source"], "subagent");
        assert_eq!(draft.payload["parent_thread_id"], "parent-session");
        assert!(
            draft.meta[CANONICAL_JSON_META_KEY]
                .as_str()
                .unwrap()
                .contains("parent-session")
        );
    }

    #[test]
    fn malformed_line_is_audited_without_dropping_valid_rows() {
        let jsonl = fixture() + "\n{not-json";
        let batch = importer().import_jsonl_str(&jsonl, "bad.jsonl").unwrap();

        assert_eq!(batch.drafts.len(), 3);
        assert_eq!(batch.audit.malformed_lines.len(), 1);
        assert!(
            batch.audit.malformed_lines[0]
                .reason
                .contains("MalformedTranscriptLine")
        );
    }

    #[test]
    fn legacy_session_meta_uses_measured_id_as_session_and_transcript_id() {
        let meta = serde_json::json!({
            "id": "legacy-session",
            "timestamp": "2025-12-01T00:00:00.000Z",
            "cwd": "D:\\repo",
            "originator": "codex-tui",
            "cli_version": "legacy",
            "instructions": "redacted by test",
            "source": "cli"
        });
        let draft = import_one_legacy_message(meta);

        assert_eq!(draft.payload["session_id"], "legacy-session");
        assert_eq!(draft.payload["transcript_id"], "legacy-session");
        assert_eq!(draft.payload["thread_source"], "legacy-main");
    }

    #[test]
    fn pre_session_id_meta_preserves_measured_thread_source() {
        let meta = serde_json::json!({
            "id": "pre-session-id",
            "timestamp": "2026-05-20T00:00:00.000Z",
            "cwd": "D:\\repo",
            "originator": "Codex Desktop",
            "cli_version": "pre-session",
            "source": "vscode",
            "thread_source": "user",
            "model_provider": "openai"
        });
        let draft = import_one_legacy_message(meta);

        assert_eq!(draft.payload["session_id"], "pre-session-id");
        assert_eq!(draft.payload["transcript_id"], "pre-session-id");
        assert_eq!(draft.payload["thread_source"], "user");
    }

    #[test]
    fn session_id_vscode_meta_without_thread_source_is_user_thread() {
        let meta = serde_json::json!({
            "session_id": "desktop-session",
            "id": "desktop-session",
            "timestamp": "2026-07-07T13:04:00.000Z",
            "cwd": "D:\\repo",
            "originator": "Codex Desktop",
            "cli_version": "0.142.5",
            "source": "vscode",
            "model_provider": "openai"
        });
        let draft = import_one_legacy_message(meta);

        assert_eq!(draft.payload["session_id"], "desktop-session");
        assert_eq!(draft.payload["transcript_id"], "desktop-session");
        assert_eq!(draft.payload["thread_source"], "user");
    }

    #[test]
    fn session_id_sidechain_without_thread_source_is_rejected() {
        let jsonl = serde_json::json!({
            "timestamp": "2026-07-07T13:04:00.000Z",
            "type": "session_meta",
            "payload": {
                "session_id": "child-session",
                "id": "child-session",
                "parent_thread_id": "parent-session",
                "timestamp": "2026-07-07T13:04:00.000Z",
                "cwd": "D:\\repo",
                "originator": "codex-tui",
                "source": {"subagent": {}},
                "model_provider": "openai"
            }
        })
        .to_string();

        let error = importer()
            .import_jsonl_str(&jsonl, "sidechain-missing-thread-source.jsonl")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("/thread_source must be a non-empty string")
        );
    }

    fn import_one_legacy_message(meta: serde_json::Value) -> ObservationDraft {
        let jsonl = serde_json::json!({
            "timestamp": "2025-12-01T00:00:00.000Z",
            "type": "session_meta",
            "payload": meta
        })
        .to_string()
            + "\n"
            + &serde_json::json!({
                "timestamp": "2025-12-01T00:00:01.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": "msg-legacy",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "legacy reply"}]
                }
            })
            .to_string();

        importer()
            .import_jsonl_str(&jsonl, "legacy.jsonl")
            .unwrap()
            .drafts
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    #[ignore]
    fn real_codex_archive_imports_when_env_points_to_archive() {
        let archive = std::env::var("LETHE_CODEX_ARCHIVE_E2E_PATH")
            .expect("LETHE_CODEX_ARCHIVE_E2E_PATH must point to the source archive root");
        let batch = importer().import_archive_path(Path::new(&archive)).unwrap();

        assert!(batch.audit.files_read > 0);
        assert!(batch.audit.transcripts_read > 0);
        assert!(!batch.drafts.is_empty());
    }
}
