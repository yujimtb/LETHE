//! Google Sheets adapter — row-oriented workspace-object-snapshot mapper.

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
pub const OBSERVER_ID: &str = "obs:gsheets-crawler";
pub const SOURCE_SYSTEM: &str = "sys:google-sheets";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleSheet {
    pub spreadsheet_id: String,
    pub revision_id: String,
    pub title: String,
    pub modified_time: DateTime<Utc>,
    pub canonical_uri: String,
    #[serde(default)]
    pub sheets: Vec<SheetTab>,
    #[serde(default)]
    pub form_response_sheet: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SheetTab {
    pub name: String,
    #[serde(default)]
    pub rows: Vec<Vec<String>>,
}

pub trait GoogleSheetsClient {
    fn get_spreadsheet(&self, spreadsheet_id: &str) -> Result<GoogleSheet, AdapterError>;
    fn list_revisions(&self, spreadsheet_id: &str) -> Result<Vec<String>, AdapterError>;
}

#[derive(Debug, Default)]
pub struct FixtureGoogleSheetsClient {
    pub sheets: HashMap<String, GoogleSheet>,
}

impl FixtureGoogleSheetsClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_spreadsheet(mut self, sheet: GoogleSheet) -> Self {
        self.sheets.insert(sheet.spreadsheet_id.clone(), sheet);
        self
    }
}

impl GoogleSheetsClient for FixtureGoogleSheetsClient {
    fn get_spreadsheet(&self, spreadsheet_id: &str) -> Result<GoogleSheet, AdapterError> {
        self.sheets
            .get(spreadsheet_id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("spreadsheet {spreadsheet_id} not found")))
    }

    fn list_revisions(&self, spreadsheet_id: &str) -> Result<Vec<String>, AdapterError> {
        Ok(vec![self.get_spreadsheet(spreadsheet_id)?.revision_id])
    }
}

pub struct GoogleSheetsAdapter<C: GoogleSheetsClient> {
    pub client: C,
    pub config: AdapterConfig,
    pub cursors: HashMap<String, String>,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl<C: GoogleSheetsClient> GoogleSheetsAdapter<C> {
    pub fn new(client: C, config: AdapterConfig) -> Self {
        Self {
            client,
            config,
            cursors: HashMap::new(),
            last_successful_capture: None,
        }
    }

    pub fn map_spreadsheet(&self, sheet: &GoogleSheet) -> ObservationDraft {
        let tabs = sheet
            .sheets
            .iter()
            .map(|tab| {
                let headers = tab.rows.first().cloned().unwrap_or_default();
                let rows = tab
                    .rows
                    .iter()
                    .enumerate()
                    .skip(1)
                    .map(|(idx, row)| {
                        let cells = row
                            .iter()
                            .enumerate()
                            .map(|(col, value)| {
                                serde_json::json!({
                                    "header": headers.get(col),
                                    "value": value,
                                })
                            })
                            .collect::<Vec<_>>();
                        serde_json::json!({
                            "rowNumber": idx + 1,
                            "cells": cells,
                        })
                    })
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "name": tab.name,
                    "headers": headers,
                    "rows": rows,
                })
            })
            .collect::<Vec<_>>();
        let payload = serde_json::json!({
            "title": sheet.title,
            "artifact": {
                "provider": "google",
                "service": "sheets",
                "objectType": "spreadsheet",
                "sourceObjectId": sheet.spreadsheet_id,
                "canonicalUri": sheet.canonical_uri,
            },
            "revision": {
                "sourceRevisionId": sheet.revision_id,
                "sourceModifiedAt": sheet.modified_time,
                "captureMode": "snapshot",
            },
            "native": {
                "encoding": "inline-json",
                "tabs": tabs,
            },
            "metadata": {
                "formResponseSheet": sheet.form_response_sheet,
            },
        });
        let object_id = format!(
            "spreadsheet:{}:revision:{}",
            sheet.spreadsheet_id, sheet.revision_id
        );
        let canonical_json = canonical_json(&serde_json::json!({
            "spreadsheet_id": sheet.spreadsheet_id,
            "revision_id": sheet.revision_id,
            "tabs": tabs,
        }));
        ObservationDraft {
            schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
            schema_version: SemVer::new(WORKSPACE_SNAPSHOT_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new(format!("document:gsheets:{}", sheet.spreadsheet_id)),
            target: None,
            payload,
            attachments: vec![],
            published: sheet.modified_time,
            idempotency_key: identity_key("google-sheets", &object_id, &canonical_json),
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
                OBJECT_ID_META_KEY: object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
                "source_container": "google-sheets",
                "form_response_sheet": sheet.form_response_sheet,
            }),
        }
    }
}

impl<C: GoogleSheetsClient> SourceAdapter for GoogleSheetsAdapter<C> {
    fn fetch_incremental(&self, _cursor: Option<&Cursor>) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "GoogleSheetsAdapter::fetch_incremental requires explicit spreadsheet IDs".into(),
        ))
    }

    fn fetch_snapshot(&self, target_id: &str) -> FetchResult {
        match self.client.get_spreadsheet(target_id) {
            Ok(sheet) => FetchResult::Ok {
                items: vec![RawData {
                    data: serde_json::to_value(sheet).unwrap_or_default(),
                    blobs: vec![],
                }],
                next_cursor: None,
                has_more: false,
            },
            Err(err) => FetchResult::Error(err),
        }
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        let sheet = serde_json::from_value::<GoogleSheet>(raw.data.clone()).map_err(|err| {
            AdapterError::MalformedResponse {
                message: format!("Google Sheets raw data is not a spreadsheet: {err}"),
            }
        })?;
        Ok(vec![self.map_spreadsheet(&sheet)])
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
    use lethe_adapter_api::conformance::source_adapter_contract;
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
    fn spreadsheet_maps_rows_with_header_context() {
        let sheet = GoogleSheet {
            spreadsheet_id: "sheet1".into(),
            revision_id: "rev1".into(),
            title: "Roster".into(),
            modified_time: Utc::now(),
            canonical_uri: "https://docs.google.com/spreadsheets/d/sheet1".into(),
            sheets: vec![SheetTab {
                name: "Members".into(),
                rows: vec![
                    vec!["name".into(), "role".into()],
                    vec!["Ada".into(), "admin".into()],
                ],
            }],
            form_response_sheet: false,
        };
        let adapter = GoogleSheetsAdapter::new(FixtureGoogleSheetsClient::new(), config());
        let raw = RawData {
            data: serde_json::to_value(&sheet).unwrap(),
            blobs: vec![],
        };
        source_adapter_contract(&adapter, &raw, None);
        let draft = adapter.map_spreadsheet(&sheet);
        assert_eq!(draft.payload["artifact"]["service"], "sheets");
        assert_eq!(
            draft.payload["native"]["tabs"][0]["rows"][0]["cells"][0]["header"],
            "name"
        );
    }
}
