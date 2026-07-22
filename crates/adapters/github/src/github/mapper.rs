//! GitHub dump -> Observation mapper.
//!
//! Fetching stays outside this crate. The mapper is a pure transformation from
//! a dumped JSON bundle to ObservationDrafts.

use chrono::{DateTime, SecondsFormat, Utc};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::idempotency::{
    CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, canonical_json, identity_key,
    normalize_canonical_body,
};
use lethe_adapter_api::traits::ObservationDraft;
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};
use serde::Deserialize;

pub const GITHUB_EVENT_SCHEMA: &str = "schema:github-event";
pub const GITHUB_EVENT_SCHEMA_VERSION: &str = "1.0.0";
pub const GITHUB_OBSERVER_ID: &str = "obs:github-importer";
pub const GITHUB_SOURCE_SYSTEM: &str = "sys:github";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitHubDump {
    #[serde(default)]
    pub dumped_at: Option<String>,
    pub repositories: Vec<RepositoryDump>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryDump {
    pub full_name: String,
    #[serde(default)]
    pub issues: Vec<serde_json::Value>,
    #[serde(default)]
    pub issue_comments: Vec<serde_json::Value>,
    #[serde(default)]
    pub pull_requests: Vec<serde_json::Value>,
    #[serde(default)]
    pub pull_request_reviews: Vec<serde_json::Value>,
    #[serde(default)]
    pub pull_request_review_comments: Vec<serde_json::Value>,
    #[serde(default)]
    pub commits: Vec<serde_json::Value>,
    #[serde(default)]
    pub timeline_events: Vec<serde_json::Value>,
}

pub struct GitHubDumpMapper {
    adapter_version: SemVer,
}

impl GitHubDumpMapper {
    pub fn new(adapter_version: SemVer) -> Self {
        Self { adapter_version }
    }

    pub fn import_json_str(&self, json: &str) -> Result<Vec<ObservationDraft>, AdapterError> {
        let dump =
            serde_json::from_str::<GitHubDump>(json).map_err(|err| malformed(err.to_string()))?;
        self.map_dump(&dump)
    }

    pub fn map_dump(&self, dump: &GitHubDump) -> Result<Vec<ObservationDraft>, AdapterError> {
        let mut drafts = Vec::new();
        for repo in &dump.repositories {
            require_non_empty("repository.full_name", &repo.full_name)?;

            for issue in &repo.issues {
                if issue.get("pull_request").is_some() {
                    continue;
                }
                drafts.push(self.map_issue(&repo.full_name, issue)?);
            }
            for comment in &repo.issue_comments {
                drafts.push(self.map_issue_comment(&repo.full_name, comment)?);
            }
            for pull_request in &repo.pull_requests {
                drafts.push(self.map_pull_request(&repo.full_name, pull_request)?);
            }
            for review in &repo.pull_request_reviews {
                drafts.push(self.map_pull_request_review(&repo.full_name, review)?);
            }
            for comment in &repo.pull_request_review_comments {
                drafts.push(self.map_pull_request_review_comment(&repo.full_name, comment)?);
            }
            for commit in &repo.commits {
                drafts.push(self.map_commit(&repo.full_name, commit)?);
            }
            for event in &repo.timeline_events {
                drafts.push(self.map_timeline_event(&repo.full_name, event)?);
            }
        }
        Ok(drafts)
    }

    fn map_issue(
        &self,
        repo: &str,
        issue: &serde_json::Value,
    ) -> Result<ObservationDraft, AdapterError> {
        let number = required_i64(issue, "/number")?;
        let object_id = format!("{repo}#issue#{number}");
        let published = required_datetime(issue, "/created_at")?;
        let title = required_str(issue, "/title")?;
        let body = required_nullable_str(issue, "/body")?;
        let canonical = serde_json::json!({
            "title": normalize_canonical_body(title),
            "body": normalize_canonical_body(body),
        });
        let payload = serde_json::json!({
            "object_type": "issue",
            "repo": repo,
            "number": number,
            "title": title,
            "body": body,
            "state": optional_str(issue, "/state"),
            "created_at": published,
            "updated_at": optional_str(issue, "/updated_at"),
            "author": actor_login(issue.get("user")),
        });
        Ok(self.draft(repo, "issue", object_id, canonical, payload, published))
    }

    fn map_issue_comment(
        &self,
        repo: &str,
        comment: &serde_json::Value,
    ) -> Result<ObservationDraft, AdapterError> {
        let id = required_i64(comment, "/id")?;
        let object_id = format!("{repo}#issue_comment#{id}");
        let published = required_datetime(comment, "/created_at")?;
        let body = required_nullable_str(comment, "/body")?;
        let canonical = serde_json::json!({
            "body": normalize_canonical_body(body),
        });
        let payload = serde_json::json!({
            "object_type": "issue_comment",
            "repo": repo,
            "id": id,
            "body": body,
            "created_at": published,
            "updated_at": optional_str(comment, "/updated_at"),
            "author": actor_login(comment.get("user")),
        });
        Ok(self.draft(
            repo,
            "issue_comment",
            object_id,
            canonical,
            payload,
            published,
        ))
    }

    fn map_pull_request(
        &self,
        repo: &str,
        pull_request: &serde_json::Value,
    ) -> Result<ObservationDraft, AdapterError> {
        let number = required_i64(pull_request, "/number")?;
        let object_id = format!("{repo}#pr#{number}");
        let published = required_datetime(pull_request, "/created_at")?;
        let title = required_str(pull_request, "/title")?;
        let body = required_nullable_str(pull_request, "/body")?;
        let canonical = serde_json::json!({
            "title": normalize_canonical_body(title),
            "body": normalize_canonical_body(body),
        });
        let payload = serde_json::json!({
            "object_type": "pull_request",
            "repo": repo,
            "number": number,
            "title": title,
            "body": body,
            "state": optional_str(pull_request, "/state"),
            "created_at": published,
            "updated_at": optional_str(pull_request, "/updated_at"),
            "author": actor_login(pull_request.get("user")),
            "head_sha": optional_str(pull_request, "/head/sha"),
            "base_sha": optional_str(pull_request, "/base/sha"),
        });
        Ok(self.draft(
            repo,
            "pull_request",
            object_id,
            canonical,
            payload,
            published,
        ))
    }

    fn map_pull_request_review(
        &self,
        repo: &str,
        review: &serde_json::Value,
    ) -> Result<ObservationDraft, AdapterError> {
        let id = required_i64(review, "/id")?;
        let object_id = format!("{repo}#pr_review#{id}");
        let published = required_datetime(review, "/submitted_at")?;
        let state = required_str(review, "/state")?;
        let body = required_nullable_str(review, "/body")?;
        let canonical = serde_json::json!({
            "state": state,
            "body": normalize_canonical_body(body),
        });
        let payload = serde_json::json!({
            "object_type": "pull_request_review",
            "repo": repo,
            "id": id,
            "state": state,
            "body": body,
            "submitted_at": published,
            "author": actor_login(review.get("user")),
            "commit_id": optional_str(review, "/commit_id"),
        });
        Ok(self.draft(
            repo,
            "pull_request_review",
            object_id,
            canonical,
            payload,
            published,
        ))
    }

    fn map_pull_request_review_comment(
        &self,
        repo: &str,
        comment: &serde_json::Value,
    ) -> Result<ObservationDraft, AdapterError> {
        let id = required_i64(comment, "/id")?;
        let object_id = format!("{repo}#pr_review_comment#{id}");
        let published = required_datetime(comment, "/created_at")?;
        let body = required_nullable_str(comment, "/body")?;
        let path = required_str(comment, "/path")?;
        let line = required_i64_any(comment, &["/line", "/original_line"])?;
        let anchor_sha =
            required_str_any(comment, &["/original_commit_id", "/commit_id"])?.to_owned();
        let canonical = serde_json::json!({
            "body": normalize_canonical_body(body),
            "anchor": {
                "path": path,
                "line": line,
                "anchor_sha": anchor_sha.clone(),
            },
        });
        let payload = serde_json::json!({
            "object_type": "pull_request_review_comment",
            "repo": repo,
            "id": id,
            "body": body,
            "path": path,
            "line": line,
            "anchor_sha": anchor_sha,
            "created_at": published,
            "updated_at": optional_str(comment, "/updated_at"),
            "author": actor_login(comment.get("user")),
            "pull_request_review_id": optional_i64(comment, "/pull_request_review_id"),
        });
        Ok(self.draft(
            repo,
            "pull_request_review_comment",
            object_id,
            canonical,
            payload,
            published,
        ))
    }

    fn map_commit(
        &self,
        repo: &str,
        commit: &serde_json::Value,
    ) -> Result<ObservationDraft, AdapterError> {
        let sha = required_str(commit, "/sha")?;
        let object_id = format!("{repo}#commit#{sha}");
        let published = required_datetime(commit, "/commit/author/date")?;
        let message = required_str(commit, "/commit/message")?;
        let author = serde_json::json!({
            "name": required_str(commit, "/commit/author/name")?,
            "email": required_str(commit, "/commit/author/email")?,
            "login": actor_login(commit.get("author")),
        });
        let changed_files = commit
            .get("files")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| malformed("commit /files must be an array".to_owned()))?
            .iter()
            .map(sanitize_commit_file)
            .collect::<Result<Vec<_>, AdapterError>>()?;
        let author_date = published.to_rfc3339_opts(SecondsFormat::Micros, true);
        let canonical = serde_json::json!({
            "message": normalize_canonical_body(message),
            "author": author.clone(),
            "author_date": author_date,
            "changed_files": changed_files.clone(),
        });
        let payload = serde_json::json!({
            "object_type": "commit",
            "repo": repo,
            "sha": sha,
            "message": message,
            "author": author,
            "author_date": published,
            "committer_date": optional_str(commit, "/commit/committer/date"),
            "changed_files": changed_files,
        });
        Ok(self.draft(repo, "commit", object_id, canonical, payload, published))
    }

    fn map_timeline_event(
        &self,
        repo: &str,
        event: &serde_json::Value,
    ) -> Result<ObservationDraft, AdapterError> {
        let event_type = required_str(event, "/event")?;
        let event_key = timeline_event_key(event_type, event)?;
        let object_id = format!("{repo}#issue_event#{event_key}");
        let published = timeline_event_published(event_type, event)?;
        let attribution = timeline_event_attribution(event_type, event)?;
        let fields = sanitize_timeline_fields(event)?;
        let canonical = serde_json::json!({
            "event_type": event_type,
            "attribution": attribution.clone(),
            "fields": fields.clone(),
        });
        let payload = serde_json::json!({
            "object_type": "timeline_event",
            "repo": repo,
            "event_key": event_key,
            "event_type": event_type,
            "attribution": attribution,
            "created_at": published,
            "fields": fields,
        });
        Ok(self.draft(
            repo,
            "timeline_event",
            object_id,
            canonical,
            payload,
            published,
        ))
    }

    fn draft(
        &self,
        repo: &str,
        object_type: &str,
        object_id: String,
        canonical_tuple: serde_json::Value,
        payload: serde_json::Value,
        published: DateTime<Utc>,
    ) -> ObservationDraft {
        let canonical_json = canonical_json(&canonical_tuple);
        let idempotency_key = identity_key("github", &object_id, &canonical_json);
        ObservationDraft {
            schema: SchemaRef::new(GITHUB_EVENT_SCHEMA),
            schema_version: SemVer::new(GITHUB_EVENT_SCHEMA_VERSION),
            observer: ObserverRef::new(GITHUB_OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(GITHUB_SOURCE_SYSTEM)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("github:{object_id}")),
            target: None,
            payload,
            attachments: vec![],
            published,
            idempotency_key,
            client_ref: None,
            meta: serde_json::json!({
                "sourceAdapterVersion": self.adapter_version.as_str(),
                "source_container": repo,
                "github_object_type": object_type,
                OBJECT_ID_META_KEY: object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
            }),
        }
    }
}

fn sanitize_commit_file(file: &serde_json::Value) -> Result<serde_json::Value, AdapterError> {
    Ok(serde_json::json!({
        "filename": required_str(file, "/filename")?,
        "status": required_str(file, "/status")?,
        "sha": optional_str(file, "/sha"),
        "previous_filename": optional_str(file, "/previous_filename"),
        "additions": optional_i64(file, "/additions"),
        "deletions": optional_i64(file, "/deletions"),
        "changes": optional_i64(file, "/changes"),
    }))
}

fn sanitize_timeline_fields(event: &serde_json::Value) -> Result<serde_json::Value, AdapterError> {
    let object = event
        .as_object()
        .ok_or_else(|| malformed("timeline event must be an object".to_owned()))?;
    let mut sanitized = serde_json::Map::new();
    for (key, value) in object {
        if matches!(
            key.as_str(),
            "id" | "node_id" | "url" | "event" | "actor" | "created_at"
        ) {
            continue;
        }
        sanitized.insert(key.clone(), value.clone());
    }
    Ok(serde_json::Value::Object(sanitized))
}

fn timeline_event_key(event_type: &str, event: &serde_json::Value) -> Result<String, AdapterError> {
    if let Some(id) = event.pointer("/id").and_then(serde_json::Value::as_i64) {
        return Ok(id.to_string());
    }
    if event_type == "committed" {
        return required_str(event, "/sha").map(ToOwned::to_owned);
    }
    Err(malformed(
        "timeline event requires integer /id unless event is committed with /sha".to_owned(),
    ))
}

fn timeline_event_published(
    event_type: &str,
    event: &serde_json::Value,
) -> Result<DateTime<Utc>, AdapterError> {
    if event.pointer("/created_at").is_some() {
        return required_datetime(event, "/created_at");
    }
    if event_type == "committed" {
        return required_datetime(event, "/author/date");
    }
    Err(malformed(
        "timeline event requires /created_at unless event is committed with /author/date"
            .to_owned(),
    ))
}

fn timeline_event_attribution(
    event_type: &str,
    event: &serde_json::Value,
) -> Result<serde_json::Value, AdapterError> {
    if let Some(actor) = actor_login(event.get("actor")) {
        return Ok(serde_json::Value::String(actor));
    }
    if event_type == "committed" {
        return Ok(serde_json::json!({
            "name": required_str(event, "/author/name")?,
            "email": required_str(event, "/author/email")?,
        }));
    }
    Err(malformed(
        "timeline event requires /actor/login unless event is committed with /author".to_owned(),
    ))
}

fn required_datetime(
    value: &serde_json::Value,
    pointer: &'static str,
) -> Result<DateTime<Utc>, AdapterError> {
    let raw = required_str(value, pointer)?;
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.to_utc())
        .map_err(|err| malformed(format!("{pointer} must be RFC3339: {err}")))
}

fn required_str<'a>(
    value: &'a serde_json::Value,
    pointer: &'static str,
) -> Result<&'a str, AdapterError> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| malformed(format!("{pointer} must be a string")))
        .and_then(|value| {
            require_non_empty(pointer, value)?;
            Ok(value)
        })
}

fn required_str_any<'a>(
    value: &'a serde_json::Value,
    pointers: &'static [&'static str],
) -> Result<&'a str, AdapterError> {
    for pointer in pointers {
        if let Some(raw) = value.pointer(pointer).and_then(serde_json::Value::as_str)
            && !raw.trim().is_empty()
        {
            return Ok(raw);
        }
    }
    Err(malformed(format!(
        "one of {} must be a non-empty string",
        pointers.join(", ")
    )))
}

fn required_nullable_str<'a>(
    value: &'a serde_json::Value,
    pointer: &'static str,
) -> Result<&'a str, AdapterError> {
    match value.pointer(pointer) {
        Some(serde_json::Value::Null) => Ok(""),
        Some(serde_json::Value::String(raw)) => Ok(raw),
        _ => Err(malformed(format!("{pointer} must be a string or null"))),
    }
}

fn optional_str(value: &serde_json::Value, pointer: &'static str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn required_i64(value: &serde_json::Value, pointer: &'static str) -> Result<i64, AdapterError> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| malformed(format!("{pointer} must be an integer")))
}

fn required_i64_any(
    value: &serde_json::Value,
    pointers: &'static [&'static str],
) -> Result<i64, AdapterError> {
    for pointer in pointers {
        if let Some(raw) = value.pointer(pointer).and_then(serde_json::Value::as_i64) {
            return Ok(raw);
        }
    }
    Err(malformed(format!(
        "one of {} must be an integer",
        pointers.join(", ")
    )))
}

fn optional_i64(value: &serde_json::Value, pointer: &'static str) -> Option<i64> {
    value.pointer(pointer).and_then(serde_json::Value::as_i64)
}

fn actor_login(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(|value| value.get("login"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn require_non_empty(name: &str, value: &str) -> Result<(), AdapterError> {
    if value.trim().is_empty() {
        Err(malformed(format!("{name} must not be blank")))
    } else {
        Ok(())
    }
}

fn malformed(message: String) -> AdapterError {
    AdapterError::MalformedResponse { message }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapper() -> GitHubDumpMapper {
        GitHubDumpMapper::new(SemVer::new("1.0.0"))
    }

    fn sample_dump() -> serde_json::Value {
        serde_json::json!({
            "repositories": [{
                "full_name": "owner/repo",
                "issues": [{
                    "number": 1,
                    "title": "Bug",
                    "body": "body",
                    "state": "open",
                    "created_at": "2026-07-01T00:00:00Z",
                    "updated_at": "2026-07-01T00:01:00Z",
                    "user": {"login": "alice"}
                }],
                "issue_comments": [{
                    "id": 10,
                    "body": "comment",
                    "created_at": "2026-07-01T00:02:00Z",
                    "updated_at": "2026-07-01T00:03:00Z",
                    "user": {"login": "bob"}
                }],
                "pull_requests": [{
                    "number": 2,
                    "title": "Feature",
                    "body": "pr body",
                    "state": "closed",
                    "created_at": "2026-07-01T00:04:00Z",
                    "updated_at": "2026-07-01T00:05:00Z",
                    "user": {"login": "carol"},
                    "head": {"sha": "headsha"},
                    "base": {"sha": "basesha"}
                }],
                "pull_request_reviews": [{
                    "id": 20,
                    "state": "APPROVED",
                    "body": "looks good",
                    "submitted_at": "2026-07-01T00:06:00Z",
                    "commit_id": "reviewsha",
                    "user": {"login": "dave"}
                }],
                "pull_request_review_comments": [{
                    "id": 30,
                    "body": "line note",
                    "path": "src/lib.rs",
                    "line": 7,
                    "original_commit_id": "anchorsha",
                    "created_at": "2026-07-01T00:07:00Z",
                    "updated_at": "2026-07-01T00:08:00Z",
                    "pull_request_review_id": 20,
                    "user": {"login": "erin"}
                }],
                "commits": [{
                    "sha": "commitsha",
                    "commit": {
                        "message": "commit message",
                        "author": {
                            "name": "Frank",
                            "email": "frank@example.com",
                            "date": "2026-07-01T00:09:00Z"
                        },
                        "committer": {"date": "2026-07-01T00:10:00Z"}
                    },
                    "author": {"login": "frank"},
                    "files": [{
                        "filename": "src/lib.rs",
                        "status": "modified",
                        "sha": "filesha",
                        "additions": 1,
                        "deletions": 2,
                        "changes": 3,
                        "patch": "@@ diff content"
                    }]
                }],
                "timeline_events": [{
                    "id": 40,
                    "event": "renamed",
                    "actor": {"login": "gina"},
                    "created_at": "2026-07-01T00:11:00Z",
                    "rename": {"from": "old", "to": "new"}
                }]
            }]
        })
    }

    #[test]
    fn maps_all_supported_object_types() {
        let drafts = mapper()
            .import_json_str(&sample_dump().to_string())
            .unwrap();

        assert_eq!(drafts.len(), 7);
        assert!(
            drafts
                .iter()
                .any(|draft| draft.meta["object_id"] == "owner/repo#commit#commitsha")
        );
        assert!(
            drafts
                .iter()
                .any(|draft| draft.payload["object_type"] == "timeline_event")
        );
    }

    #[test]
    fn commit_payload_and_canonical_tuple_exclude_diff_patch() {
        let drafts = mapper()
            .import_json_str(&sample_dump().to_string())
            .unwrap();
        let commit = drafts
            .iter()
            .find(|draft| draft.payload["object_type"] == "commit")
            .unwrap();
        let payload = commit.payload.to_string();
        let canonical = commit.meta[CANONICAL_JSON_META_KEY].as_str().unwrap();

        assert!(!payload.contains("patch"));
        assert!(!payload.contains("@@ diff content"));
        assert!(!canonical.contains("patch"));
        assert!(!canonical.contains("@@ diff content"));
    }

    #[test]
    fn same_dump_produces_same_identity_keys() {
        let json = sample_dump().to_string();
        let first = mapper().import_json_str(&json).unwrap();
        let second = mapper().import_json_str(&json).unwrap();
        let first_keys = first
            .iter()
            .map(|draft| draft.idempotency_key.as_str().to_owned())
            .collect::<Vec<_>>();
        let second_keys = second
            .iter()
            .map(|draft| draft.idempotency_key.as_str().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(first_keys, second_keys);
    }

    #[test]
    fn issue_body_edit_changes_identity_key() {
        let first = mapper()
            .import_json_str(&sample_dump().to_string())
            .unwrap()
            .into_iter()
            .find(|draft| draft.payload["object_type"] == "issue")
            .unwrap();
        let mut edited = sample_dump();
        edited["repositories"][0]["issues"][0]["body"] = serde_json::json!("changed");
        let second = mapper()
            .import_json_str(&edited.to_string())
            .unwrap()
            .into_iter()
            .find(|draft| draft.payload["object_type"] == "issue")
            .unwrap();

        assert_ne!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn unknown_timeline_event_type_is_preserved() {
        let mut dump = sample_dump();
        dump["repositories"][0]["timeline_events"][0]["event"] =
            serde_json::json!("future_event_type");

        let event = mapper()
            .import_json_str(&dump.to_string())
            .unwrap()
            .into_iter()
            .find(|draft| draft.payload["object_type"] == "timeline_event")
            .unwrap();

        assert_eq!(event.payload["event_type"], "future_event_type");
        assert_eq!(event.payload["fields"]["rename"]["from"], "old");
    }

    #[test]
    fn committed_timeline_event_uses_sha_key_and_author_date() {
        let dump = serde_json::json!({
            "repositories": [{
                "full_name": "owner/repo",
                "timeline_events": [{
                    "event": "committed",
                    "sha": "timelinecommitsha",
                    "node_id": "node",
                    "message": "timeline commit",
                    "author": {
                        "name": "A. U. Thor",
                        "email": "author@example.com",
                        "date": "2026-07-01T00:12:00Z"
                    },
                    "committer": {
                        "name": "C. O. Mmitter",
                        "email": "committer@example.com",
                        "date": "2026-07-01T00:13:00Z"
                    },
                    "parents": [],
                    "tree": {"sha": "tree"}
                }]
            }]
        });

        let event = mapper()
            .import_json_str(&dump.to_string())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(
            event.meta[OBJECT_ID_META_KEY],
            "owner/repo#issue_event#timelinecommitsha"
        );
        assert_eq!(event.published.to_rfc3339(), "2026-07-01T00:12:00+00:00");
        assert_eq!(event.payload["event_key"], "timelinecommitsha");
        assert_eq!(event.payload["attribution"]["email"], "author@example.com");
    }
}
