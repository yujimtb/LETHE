//! Google Forms adapter — form structure, response fact, and response content split.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use lethe_adapter_api::config::AdapterConfig;
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::heartbeat::heartbeat_draft;
use lethe_adapter_api::idempotency::{
    CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, canonical_json, identity_key,
};
use lethe_adapter_api::traits::{Cursor, FetchResult, ObservationDraft, RawData, SourceAdapter};
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};
use serde::{Deserialize, Serialize};

pub const WORKSPACE_SNAPSHOT_SCHEMA: &str = "schema:workspace-object-snapshot";
pub const WORKSPACE_SNAPSHOT_SCHEMA_VERSION: &str = "1.0.0";
pub const OBSERVER_ID: &str = "obs:gforms-crawler";
pub const SOURCE_SYSTEM: &str = "sys:google-forms";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleForm {
    pub form_id: String,
    pub revision_id: String,
    pub title: String,
    pub description: Option<String>,
    pub canonical_uri: String,
    pub modified_time: DateTime<Utc>,
    #[serde(default)]
    pub questions: Vec<FormQuestion>,
    #[serde(default)]
    pub linked_sheet_id: Option<String>,
    #[serde(default)]
    pub responses: Vec<FormResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormQuestion {
    pub question_id: String,
    pub title: String,
    pub question_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormResponse {
    pub response_id: String,
    pub respondent: String,
    pub submitted_at: DateTime<Utc>,
    #[serde(default)]
    pub answers: serde_json::Value,
}

pub trait GoogleFormsClient {
    fn get_form(&self, form_id: &str) -> Result<GoogleForm, AdapterError>;
    fn list_responses(&self, form_id: &str) -> Result<Vec<FormResponse>, AdapterError>;
}

#[derive(Debug, Default)]
pub struct FixtureGoogleFormsClient {
    pub forms: HashMap<String, GoogleForm>,
}

impl FixtureGoogleFormsClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_form(mut self, form: GoogleForm) -> Self {
        self.forms.insert(form.form_id.clone(), form);
        self
    }
}

impl GoogleFormsClient for FixtureGoogleFormsClient {
    fn get_form(&self, form_id: &str) -> Result<GoogleForm, AdapterError> {
        self.forms
            .get(form_id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("form {form_id} not found")))
    }

    fn list_responses(&self, form_id: &str) -> Result<Vec<FormResponse>, AdapterError> {
        Ok(self.get_form(form_id)?.responses)
    }
}

pub struct GoogleFormsAdapter<C: GoogleFormsClient> {
    pub client: C,
    pub config: AdapterConfig,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl<C: GoogleFormsClient> GoogleFormsAdapter<C> {
    pub fn new(client: C, config: AdapterConfig) -> Self {
        Self {
            client,
            config,
            last_successful_capture: None,
        }
    }

    pub fn map_form(&self, form: &GoogleForm) -> Vec<ObservationDraft> {
        let mut drafts = vec![self.form_structure_draft(form)];
        for response in &form.responses {
            drafts.push(self.response_fact_draft(form, response));
            drafts.push(self.response_content_draft(form, response));
        }
        drafts
    }

    fn form_structure_draft(&self, form: &GoogleForm) -> ObservationDraft {
        let payload = serde_json::json!({
            "title": form.title,
            "description": form.description,
            "artifact": {
                "provider": "google",
                "service": "forms",
                "objectType": "form",
                "sourceObjectId": form.form_id,
                "canonicalUri": form.canonical_uri,
            },
            "revision": {
                "sourceRevisionId": form.revision_id,
                "sourceModifiedAt": form.modified_time,
                "captureMode": "snapshot",
            },
            "native": {
                "encoding": "inline-json",
                "questions": form.questions,
            },
            "metadata": {
                "linkedSheetId": form.linked_sheet_id,
            },
        });
        self.draft(
            form,
            "form",
            &format!("form:{}:revision:{}", form.form_id, form.revision_id),
            form.modified_time,
            payload,
            serde_json::json!({
                "form_id": form.form_id,
                "revision_id": form.revision_id,
                "questions": form.questions,
            }),
        )
    }

    fn response_fact_draft(&self, form: &GoogleForm, response: &FormResponse) -> ObservationDraft {
        let payload = serde_json::json!({
            "title": form.title,
            "artifact": {
                "provider": "google",
                "service": "forms",
                "objectType": "form-response-fact",
                "sourceObjectId": form.form_id,
                "canonicalUri": form.canonical_uri,
            },
            "response": {
                "responseId": response.response_id,
                "respondent": response.respondent,
                "submittedAt": response.submitted_at,
            },
            "metadata": {
                "linkedSheetId": form.linked_sheet_id,
            },
        });
        self.draft(
            form,
            "form-response-fact",
            &format!(
                "form:{}:response:{}:fact",
                form.form_id, response.response_id
            ),
            response.submitted_at,
            payload,
            serde_json::json!({
                "form_id": form.form_id,
                "response_id": response.response_id,
                "respondent": response.respondent,
                "submitted_at": response.submitted_at,
            }),
        )
    }

    fn response_content_draft(
        &self,
        form: &GoogleForm,
        response: &FormResponse,
    ) -> ObservationDraft {
        let payload = serde_json::json!({
            "title": form.title,
            "artifact": {
                "provider": "google",
                "service": "forms",
                "objectType": "form-response-content",
                "sourceObjectId": form.form_id,
                "canonicalUri": form.canonical_uri,
            },
            "response": {
                "responseId": response.response_id,
                "respondent": response.respondent,
                "submittedAt": response.submitted_at,
                "answers": response.answers,
            },
        });
        self.draft(
            form,
            "form-response-content",
            &format!(
                "form:{}:response:{}:content",
                form.form_id, response.response_id
            ),
            response.submitted_at,
            payload,
            serde_json::json!({
                "form_id": form.form_id,
                "response_id": response.response_id,
                "answers": response.answers,
            }),
        )
    }

    fn draft(
        &self,
        form: &GoogleForm,
        object_type: &str,
        object_id: &str,
        published: DateTime<Utc>,
        payload: serde_json::Value,
        canonical_tuple: serde_json::Value,
    ) -> ObservationDraft {
        let canonical_json = canonical_json(&canonical_tuple);
        ObservationDraft {
            schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
            schema_version: SemVer::new(WORKSPACE_SNAPSHOT_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new(format!("document:gforms:{}", form.form_id)),
            target: None,
            payload,
            attachments: vec![],
            published,
            idempotency_key: identity_key("google-forms", object_id, &canonical_json),
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
                OBJECT_ID_META_KEY: object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
                "source_container": "google-forms",
                "form_object_type": object_type,
                "linked_sheet_id": form.linked_sheet_id,
            }),
        }
    }
}

impl<C: GoogleFormsClient> SourceAdapter for GoogleFormsAdapter<C> {
    fn fetch_incremental(&self, _cursor: Option<&Cursor>) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "GoogleFormsAdapter::fetch_incremental requires explicit form IDs".into(),
        ))
    }

    fn fetch_snapshot(&self, target_id: &str) -> FetchResult {
        match self.client.get_form(target_id) {
            Ok(form) => FetchResult::Ok {
                items: vec![RawData {
                    data: serde_json::to_value(form).unwrap_or_default(),
                    blobs: vec![],
                }],
                next_cursor: None,
                has_more: false,
            },
            Err(err) => FetchResult::Error(err),
        }
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        let form = serde_json::from_value::<GoogleForm>(raw.data.clone()).map_err(|err| {
            AdapterError::MalformedResponse {
                message: format!("Google Forms raw data is not a form: {err}"),
            }
        })?;
        Ok(self.map_form(&form))
    }

    fn heartbeat(&self) -> ObservationDraft {
        heartbeat_draft(
            &ObserverRef::new(OBSERVER_ID),
            &SourceSystemRef::new(SOURCE_SYSTEM),
            Utc::now(),
            0,
            self.last_successful_capture,
        )
    }

    fn observer_ref(&self) -> &ObserverRef {
        &self.config.observer_id
    }

    fn source_system_ref(&self) -> &SourceSystemRef {
        &self.config.source_system_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_adapter_api::config::{BackoffStrategy, RateLimitConfig, RetryConfig, SchemaBinding};
    use std::time::Duration;

    fn config() -> AdapterConfig {
        AdapterConfig {
            observer_id: ObserverRef::new(OBSERVER_ID),
            source_system_id: SourceSystemRef::new(SOURCE_SYSTEM),
            adapter_version: SemVer::new("1.0.0"),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            schemas: vec![SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA)],
            schema_bindings: vec![SchemaBinding {
                schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
                versions: ">=1.0.0 <2.0.0".into(),
            }],
            poll_interval: Duration::from_secs(86400),
            heartbeat_interval: Duration::from_secs(60),
            rate_limit: RateLimitConfig {
                requests_per_second: 10,
                burst: 5,
            },
            retry: RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential,
                max_wait: Duration::from_secs(60),
            },
            credential_ref: "secret:google".into(),
        }
    }

    #[test]
    fn response_fact_and_content_are_separate_observations() {
        let form = GoogleForm {
            form_id: "form1".into(),
            revision_id: "rev1".into(),
            title: "Survey".into(),
            description: None,
            canonical_uri: "https://docs.google.com/forms/d/form1".into(),
            modified_time: Utc::now(),
            questions: vec![FormQuestion {
                question_id: "q1".into(),
                title: "Secret?".into(),
                question_type: "text".into(),
            }],
            linked_sheet_id: Some("sheet1".into()),
            responses: vec![FormResponse {
                response_id: "r1".into(),
                respondent: "ada@example.com".into(),
                submitted_at: Utc::now(),
                answers: serde_json::json!({"q1": "yes"}),
            }],
        };
        let adapter = GoogleFormsAdapter::new(FixtureGoogleFormsClient::new(), config());
        let drafts = adapter.map_form(&form);
        assert_eq!(drafts.len(), 3);
        assert_eq!(
            drafts[1].payload["artifact"]["objectType"],
            "form-response-fact"
        );
        assert_eq!(
            drafts[2].payload["artifact"]["objectType"],
            "form-response-content"
        );
        assert!(drafts[1].payload["response"].get("answers").is_none());
    }
}
